from setuptools import setup

from cc_link_extractor.utils.consts import PACKAGE_NAME, PACKAGE_NAME_APP

setup(
    name=PACKAGE_NAME_APP,
    version="0.3.0",
    packages=[
        PACKAGE_NAME,
        f"{PACKAGE_NAME}.cc",
        f"{PACKAGE_NAME}.db",
        f"{PACKAGE_NAME}.utils"
    ],
    package_dir={PACKAGE_NAME: "src"},
    package_data={PACKAGE_NAME: ["data/*.txt"]},
    url=f"https://github.com/claudio-db/{PACKAGE_NAME_APP}",
    author="claudio-db",
    description="Toy pipeline to extract external links from CommonCrawl and analyze them",
    install_requires=[
        "requests",
        "warcio",
        "pyspark",
        "ipwhois",
        "psycopg2-binary",
        "pydantic",
        "python-whois"
    ],
    setup_requires=["flake8", "pytest-runner"],
    tests_require=["pytest"],
    entry_points={
        "console_scripts": [f"{PACKAGE_NAME} = {PACKAGE_NAME}.main:main"]
    }
)
