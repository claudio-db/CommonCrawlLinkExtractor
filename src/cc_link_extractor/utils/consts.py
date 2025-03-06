# Step names
EXTRACT = "extract"
ANALYZE = "analyze"

# Database
DATABASE_CONFIG = "db_properties"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5432

# CC indices
CC_COLLECTION_INFO = "https://index.commoncrawl.org/collinfo.json"
CC_SEGMENT_INDEX_TEMPLATE = "https://index.commoncrawl.org/{segment_id}-index"
CDX_API_URL_TEMPLATE = "{index_url}?output=json"

# WARC files
WARC_BASE_URL = "https://data.commoncrawl.org/"
WARC_TARGET_URI = "WARC-Target-URI"

# Link processing
HTML_LINK_REGEX = r'href=[\'"]?([^\'" >]+)'

# Website metrics
WEBSITE_COL = "website"
N_LINKS_COL = "n_links"
COUNTRY_COL = "country"
CATEGORY_COL = "category"
AGE_COL = "age"

# Website categorization
SUPPORTED_CATEGORIZATION_SERVICES = {
    "whoisxmlapi": "https://website-categorization.whoisxmlapi.com/api/v1?apiKey={api_key}&domainName={website}"
}