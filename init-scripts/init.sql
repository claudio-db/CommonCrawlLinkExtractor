-- Setup new user and database
CREATE USER cc_link_extractor WITH PASSWORD 'cc_20250302';

CREATE DATABASE cc_raw_links_db;

GRANT ALL PRIVILEGES ON DATABASE cc_raw_links_db TO cc_link_extractor;
