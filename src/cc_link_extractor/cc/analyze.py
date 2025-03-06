import json
import logging
import socket
import time
from datetime import datetime
from typing import Any, Dict, Optional

import requests
import whois
from ipwhois import IPWhois
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, StringType

from cc_link_extractor.utils.consts import (
    AGE_COL,
    CATEGORY_COL,
    COUNTRY_COL,
    N_LINKS_COL,
    SUPPORTED_CATEGORIZATION_SERVICES,
    WEBSITE_COL
)


def get_country_from_website(website: str, options: str) -> Optional[str]:
    """Retrieve the country code from a website using RDAP"""
    options = json.loads(options)
    max_retries = options.get("max_retries", 10)
    try:
        ip_address = socket.gethostbyname(website)
    except socket.gaierror as e:
        logging.error(f"Unable to parse website URL '{website}': {e}")
        return None

    res = {}
    for _ in range(max_retries):
        try:
            res = IPWhois(ip_address).lookup_rdap()
            break
        except Exception as e:
            logging.error(f"Error: {e}, retrying...")
            time.sleep(5)

    return res.get("asn_country_code")


def get_category_from_website(website: str, options: str) -> Optional[str]:
    """Retrieve the category of a website using the WhoisXML API"""
    options = json.loads(options)
    service, api_key = options["service"], options["api_key"]
    # TODO: For now only whoisxmlapi is supported. Make this configurable
    assert service in SUPPORTED_CATEGORIZATION_SERVICES

    url = SUPPORTED_CATEGORIZATION_SERVICES[service].format(api_key=api_key, website=website)
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        categories = [category['name'] for category in data['categories']]
    else:
        categories = []
    # Only return the top category
    return categories[0] or None


def get_domain_age_from_website(website_url: str, options: str) -> Optional[int]:
    """Determine the age (in years) of a website from the creation date"""
    _ = json.loads(options)  # Empty for now
    domain_info = whois.whois(website_url)
    creation_date = domain_info.creation_date
    if isinstance(creation_date, list):
        creation_date = creation_date[0]
    age = (datetime.now() - creation_date).days // 365 if creation_date else creation_date
    return age


ANALYZE_STEPS = {
    COUNTRY_COL: f.udf(get_country_from_website, returnType=StringType()),
    CATEGORY_COL: f.udf(get_category_from_website, returnType=StringType()),
    AGE_COL: f.udf(get_domain_age_from_website, returnType=IntegerType()),
    # TODO: add more (traffic metrics, SEO metrics, page loading speed, SSL certificate, ...)
}


def analyze_web_domains(
        df_external_links: DataFrame,
        analyze_config: Dict[str, Dict[str, Any]],
        link_col: Optional[str] = None
) -> DataFrame:
    """Analyze a collection of external links and collect aggregated statistics.

    Args:
        df_external_links: Dataframe with a raw collection of external links
        analyze_config: Configuration specifying all the attributes to include and the corresponding parameters
        link_col: Name of the column with the full external links in the input dataframe (optional).
            When omitted, relies on the first column in the dataframe schema.

    Returns:
        A dataframe with websites collected from the input links and associated attributes/metrics
    """
    link_col = link_col or df_external_links.columns[0]

    # Add primary domain (website)
    df_external_links = df_external_links.withColumn(
        WEBSITE_COL,
        f.regexp_extract(f.col(link_col), r"https?://([^/]+)", 1)
    )
    df_websites = df_external_links.groupby(WEBSITE_COL).agg(
        f.count(link_col).alias(N_LINKS_COL)
    ).orderBy(f.desc(N_LINKS_COL))

    # Apply analytics from the configuration
    for analyze_step, config in analyze_config.items():
        if analyze_step in ANALYZE_STEPS:
            analyze_udf = ANALYZE_STEPS[analyze_step]
            df_websites = df_websites.withColumn(
                analyze_step, analyze_udf(f.col(WEBSITE_COL), f.lit(json.dumps(config)))
            )
        else:
            logging.error(f"'{analyze_step}' is not a supported analyze step. Skipping...")

    return df_websites

