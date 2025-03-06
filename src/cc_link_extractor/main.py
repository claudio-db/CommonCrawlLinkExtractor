"""Entry point for executing the CommonCrawl link extraction pipeline"""
import argparse
import json
import logging
from pathlib import Path
from typing import Dict, Union

from pydantic import BaseModel, ValidationError

from pyspark.sql import SparkSession

from cc_link_extractor.cc.analyze import analyze_web_domains
from cc_link_extractor.cc.fetch import collect_links_into_dataframe
from cc_link_extractor.db.db_handler import LinkDatabase
from cc_link_extractor.utils.consts import ANALYZE, DATABASE_CONFIG, EXTRACT
from cc_link_extractor.utils.spark import initialize_spark


class DatabaseConfig(BaseModel):
    database: str
    user: str
    password: str
    host: Union[str, None]
    port: Union[int, None]


class OutputConfig(BaseModel):
    format: str
    out_path: str


class ExtractConfig(BaseModel):
    table_name: str
    num_segments: int
    limit: Union[int, None]


class AnalyzeConfig(BaseModel):
    table_name: str
    steps: Dict
    output: OutputConfig


def extract(spark: SparkSession, config: Dict, db_handler: LinkDatabase) -> None:
    """Entry point for the `extract` step.

    Args:
        spark: An active Spark session
        config: Configuration parameters for the extraction
        db_handler: Wrapper to a database where links should be dumped
    """
    try:
        config = ExtractConfig(**config)
    except ValidationError as e:
        raise ValueError(f"Invalid '{EXTRACT}' configuration: {e}")

    df_external_links = collect_links_into_dataframe(
        spark=spark,
        num_segments=config.num_segments,
        limit=config.limit
    )
    db_handler.update_table_from_dataframe(
        table_name=config.table_name,
        df=df_external_links
    )


def analyze(spark: SparkSession, config: Dict, db_handler: LinkDatabase) -> None:
    """Entry point for the `analyze` step.

    Args:
        spark: An active Spark session
        config: Configuration parameters for the analysis
        db_handler: Wrapper to a database where links should be fetched
    """
    try:
        config = AnalyzeConfig(**config)
    except ValidationError as e:
        raise ValueError(f"Invalid '{ANALYZE}' configuration: {e}")

    df_external_links = db_handler.load_table_into_dataframe(
        spark=spark,
        table_name=config.table_name
    )
    df_websites = analyze_web_domains(
        df_external_links=df_external_links,
        analyze_config=config.steps
    )
    output_format, output_path = config.output.format
    # TODO: Add support for other output formats (Arrow, PostgreSQL, ...)
    assert output_format == "parquet"
    df_websites.write.mode("overwrite").parquet(output_path)


SUPPORTED_STEPS = {
    EXTRACT: (extract, f"'{EXTRACT}': Extract external links from recent Common Crawl segments into a target database"),
    ANALYZE: (analyze, f"'{ANALYZE}': Analyze the extracted links and generate metrics/attributes at the website level")
}


def main():
    parser = argparse.ArgumentParser(description='Run the CommonCrawl link extraction pipeline')
    parser.add_argument(
        "--step", "-s",
        type=str,
        required=True,
        choices=list(SUPPORTED_STEPS),
        help=f"Which pipeline step to execute. {', '.join([SUPPORTED_STEPS[step][1] for step in SUPPORTED_STEPS])}",
    )
    parser.add_argument(
        "--config", "-c",
        type=Path,
        required=True,
        help="Path to the full link extraction configuration (JSON)",
    )
    args = parser.parse_args()

    logging.info(f"Loading full configuration from {args.config}")
    with open(args.config, "r") as f_in:
        config = json.load(f_in)
    logging.debug(f"Configuration: {config}")

    # Parse database configuration
    assert DATABASE_CONFIG in config, f"A database configuration ({DATABASE_CONFIG}) should be specified!"
    try:
        db_properties = DatabaseConfig(**config[DATABASE_CONFIG])
    except ValidationError as e:
        raise ValueError(f"Invalid '{DATABASE_CONFIG}' configuration: {e}")
    db_handler = LinkDatabase(**dict(db_properties))

    # Parse step configuration
    assert args.step in config, f"No configuration specified for '{args.command}'"
    step_f, config = SUPPORTED_STEPS[args.step][0], config[args.step]

    # Initialize Spark and run
    spark = initialize_spark()
    step_f(spark=spark, config=config, db_handler=db_handler)
    spark.stop()


if __name__ == "__main__":
    main()
