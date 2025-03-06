import json
import logging
import re
import requests
import warcio
from io import BytesIO
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType
from urllib.parse import urlparse
from typing import Dict, List, Optional

from cc_link_extractor.utils.consts import (
    CC_COLLECTION_INFO,
    CC_SEGMENT_INDEX_TEMPLATE,
    CDX_API_URL_TEMPLATE,
    HTML_LINK_REGEX,
    WARC_BASE_URL,
    WARC_TARGET_URI,
)

_DEFAULT_HEADERS = {
    'User-Agent': 'python-requests/2.31.0',
    'Accept-Encoding': 'gzip, deflate',
    'Accept': '*/*',
    'Connection': 'keep-alive',
}


def _get_recent_cc_segments(
        num_segments: int
) -> List[Dict]:
    """Fetch the most recent segments from the Common Crawl collection.

    Args:
        num_segments: How many CommonCrawl segments to consider

    Returns:
        A list of the most recent segments
    """
    response = requests.get(CC_COLLECTION_INFO, headers=_DEFAULT_HEADERS)
    collections = response.json()

    # Most recent segments are at the top
    recent_segments = collections[:num_segments]
    logging.info(f"Using segments: {[segment['id'] for segment in recent_segments]}")
    return recent_segments


def _get_warc_files_from_cc_segment(
        segment_id: str,
        limit: Optional[int] = None
) -> List[str]:
    """Get the list of WARC files associated with a CC segment.

    Args:
        segment_id: ID of a CC segment to fetch (e.g. 'CC-MAIN-2025-08')
        limit: Maximum number of WARC files to request (optional)

    Returns:
       All the retrieved WARC files as stringified JSONs
    """
    index_url = CC_SEGMENT_INDEX_TEMPLATE.format(segment_id=segment_id)
    cdx_api_url = CDX_API_URL_TEMPLATE.format(index_url=index_url)
    cdx_api_url = f"{cdx_api_url}&url=*.com"
    if limit:
        # Limit for the files to request
        cdx_api_url = f"{cdx_api_url}&limit={limit}"

    response, warc_paths = requests.get(cdx_api_url, headers=_DEFAULT_HEADERS), []
    for line in response.text.strip().split('\n'):
        if line:
            warc_paths.append(line.strip())

    return warc_paths


def _extract_links_from_warc_file(
        warc_info_str: str
) -> List[str]:
    """Process a WARC file and extract all external links.

    Args:
        warc_info_str: WARC file JSON as returned by _get_warc_files_from_cc_segment

    Returns:
        List of external links extracted from the input WARC file
    """
    warc_info = json.loads(warc_info_str)

    # Fetch the WARC record using a range request
    url = WARC_BASE_URL + warc_info['filename']
    headers = {'Range': f"bytes={warc_info['offset']}-{int(warc_info['offset']) + int(warc_info['length']) - 1}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 206:  # 206 is Partial Content (success for range requests)
        logging.error(f"Failed to download {url}, status code: {response.status_code}")
        return []

    stream = BytesIO(response.content)
    external_links = []
    try:
        for record in warcio.ArchiveIterator(stream):
            if record.rec_type == 'response':
                uri = record.rec_headers.get_header(WARC_TARGET_URI)
                content = record.content_stream().read().decode('utf-8', errors='ignore')

                # Extract the base domain of the page
                base_domain = urlparse(uri).netloc
                links = re.findall(HTML_LINK_REGEX, content)

                # Filter for external links only
                for link in links:
                    try:
                        parsed_link = urlparse(link)
                        # If the link doesn't have a netloc, it might be a relative URL
                        if not parsed_link.netloc:
                            continue
                        # Check if the domains are different
                        if parsed_link.netloc != base_domain:
                            external_links.append(link)
                    except ValueError:
                        continue
        logging.info(f"Fetched {len(external_links)} from '{url}'")
    except Exception as e:
        logging.exception(f"Error processing WARC file: {e}")
    return external_links


_extract_links_from_warc_file_udf = f.udf(_extract_links_from_warc_file, returnType=ArrayType(StringType()))


def collect_links_into_dataframe(
        spark: SparkSession,
        num_segments: int,
        limit: Optional[int] = None
) -> DataFrame:
    """Collect external links from the latest CommonCrawl segments.

    Args:
        spark: An active Spark session
        num_segments: How many CommonCrawl segments to consider
        limit: Maximum number of WARC files to consider from each segment (optional)

    Returns:
        A dataframe with external links
    """
    cc_segments = _get_recent_cc_segments(num_segments=num_segments)
    warc_files = [
        _get_warc_files_from_cc_segment(
            segment_id=cc_segment["id"], limit=limit)
        for cc_segment in cc_segments
    ]
    warc_files_all = sum(warc_files, [])

    # Flatten the list and initialize dataframe
    df_warc_files = spark.createDataFrame(
        [(warc_file,) for warc_files in warc_files_all for warc_file in warc_files], ["warc_file"]
    )
    df_warc_files = df_warc_files.withColumn("link", f.explode(_extract_links_from_warc_file_udf("df_warc_files")))
    return df_warc_files

