import pathlib
from setuptools import setup, find_packages

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

setup(
    name="judah",
    version="0.0.3",
    description="A simple service-oriented ETL framework for integrations",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/sopherapps/judah",
    author="Martin Ahindura",
    author_email="team.sopherapps@gmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
    ],
    packages=find_packages(exclude=("test",)),
    include_package_data=True,
    install_requires=[
        "pydantic",
        "python-dotenv",
        "email-notifier",
        "psycopg2-binary",
        "selenium",
        "requests",
        "SQLAlchemy",
        "xml-stream",
        "webdriver-manager",
        "bonobo",
        "xlrd"
    ],
    entry_points={
    },
)
