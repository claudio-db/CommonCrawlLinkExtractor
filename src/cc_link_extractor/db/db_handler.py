import logging
from typing import Dict, Optional

import psycopg2
from pyspark.sql import DataFrame, SparkSession

from cc_link_extractor.utils.consts import DEFAULT_HOST, DEFAULT_PORT


class LinkDatabase:
    """Basic handler to update Postgres tables in an existing database"""

    def __init__(
            self,
            database: str,
            user: str,
            password: str,
            host: Optional[str] = None,
            port: Optional[int] = None
    ) -> None:
        """Initialize a TableUpdate handler.

        Args:
            database: Database name (it is assumed to exist)
            user: Owner of the input database
            password: Password associated with the database owner
            host: Host where the database lives (local by default)
            port: Port to use to communicate with the host (optional)
        """
        self._database = database
        self._user = user
        self._password = password
        self._host = host or DEFAULT_HOST
        self._port = port or DEFAULT_PORT

    @property
    def options(self) -> Dict[str, str]:
        """Options to use with Spark reader and writers"""
        return {
            "url": f"jdbc:postgresql://{self._host}:{self._port}/{self._database}",
            "driver": "org.postgresql.Driver",
            "user": self._user,
            "password": self._password
        }

    def _create_table(self, table_name: str, col_name: str) -> None:
        """Create a given table in the database if it does not exist.

        This method creates a single-column table with the specified attribute name

        Args:
            table_name: Name of the table to create
            col_name: Attribute name to use for the table column
        """
        with psycopg2.connect(
                database=self._database,
                user=self._user,
                password=self._password,
                host=self._host,
                port=self._port
        ) as conn:
            cur = conn.cursor()
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {col_name} TEXT NOT NULL
                )
                """
            )
            conn.commit()
            cur.close()

    def update_table_from_dataframe(self, table_name: str, df: DataFrame) -> None:
        """Dump the content of an input dataframe into a target table in the database.

        Args:
            table_name: Name of the table to update. It will be created if it does not exist.
            df: Dataframe with the content to add to the table.
                The dataframe is assumed to have a single column that should match the table schema.
        """
        # Initialize table if needed
        self._create_table(
            table_name=table_name,
            # Assumption: single-column table with the full external link
            col_name=df.columns[0]
        )
        options = {
            **self.options,
            "dbtable": table_name
        }
        df.write.format("jdbc").options(**options).mode("append").save()
        logging.info(f"Updated '{table_name}'")

    def load_table_into_dataframe(self, spark: SparkSession, table_name: str) -> DataFrame:
        """Load the content of an input table into a target dataframe.

        Args:
            spark: An active Spark session
            table_name: Name of the input table. It is expected to exist in the database.

        Returns:
            A dataframe with the content of the table
        """
        options = {
            **self.options,
            "dbtable": table_name
        }
        return spark.read.format("jdbc").options(**options).load()
