# judah

>  She (Leah) said, “This time I will praise the LORD”; so she named him Judah - Genesis 29: 35

judah is a service-oriented Python package to handle ETL (extract-transform-load) tasks easily.

It follows a service-oriented architectural (SOA) design.

Under the hood, it uses the nice little ETL framework called [Bonobo](https://www.bonobo-project.org/) under the hood.

**This project is still under heavy development**

## Purpose

The judah framework was created to standardize the integration or ETL (Extract-transform-load) applications that collect
energy data from multiple external sources and saves it in a warehouse.

## Links

Here are a few important links:

- [Repository](https://github.com/sopherapps/judah.git)
- [Documentation](https://github.com/sopherapps/judah/README.md)

## Languages Used

- [Python 3.6](https://www.python.org/)

## Dependencies

- [Python3.6](https://www.python.org/downloads/release/python-368/) (attempting to use > 3.6 may cause weird errors)
- [Bonobo ETL](https://www.bonobo-project.org/)
- [SqlAlchemy](https://www.sqlalchemy.org/)
- [Selenium](https://selenium-python.readthedocs.io/)
- [requests](https://requests.readthedocs.io/en/master/)
- [xml-stream](https://pypi.org/project/xml-stream/)
- [webdriver-manager](https://pypi.org/project/webdriver-manager/)
- [xlrd](https://pypi.org/project/xlrd/)
- [python-dotenv](https://pypi.org/project/python-dotenv/)
- [pydantic](https://pydantic-docs.helpmanual.io/)
- [email-notifier](https://pypi.org/project/email-notifier/)

## Getting Started

- Install the package

```bash
pip install judah
```

- Copy the `.example.env` file to `.env` and make appropriate edits on it

```bash
cp .example.env .env
```

- Import the source, destination and transformer classes, as well as any utility functions you may like and use accordingly

```python  
from judah.sources.export_site.date_based import DateBasedExportSiteSource
# ...  
```

### Expected App System Design and Architecture

**The judah framework expects all applications that use it to follow a service-oriented-architecture as shown below.**

- The app should have a `services` folder (or in python, what we call package) to contain the separate ETL services, 
    each corresponding to a given third-party data source e.g. TenneT, rte
- Subsequently, each ETL service should be divided up into child services. Each child service should represent a unique data flow path e.g. REST-API-to-database,
    REST-API-to-cache, REST-API-to-queue, file-download-site-to-database, file-download-site-to-queue etc.
- Each child service should be divided up into a number of microservices. 
    Each microservice should correspond to a single dataset, e.g. 'available_capacity', 'installed_capacity' etc.
- Each microservice is expected to have a `destination` folder, a `source.py` file, a `controller.py` file and a `transformers.py` file.
    - The `destination` folder contains the database model file to which the data is to be saved.
      It contains a child class of the DatabaseBaseModel class of the [judah framework](https://github.com/sopherapps/judah/src)
    - The `source.py` file contains a child class of the BaseSource class of the [judah framework](https://github.com/sopherapps/judah/src). 
      This is the class responsible for connecting to the data source (e.g. the REST API) and downloading the data from there.
    - The `transfomers.py` file contains child classes of the BaseTransformer class of the [judah framework](https://github.com/sopherapps/judah/src).
      They are responsible for transforming the source data into the data that can be saved. This may involve changing field names and types, exploading the data etc.
    - The `controller.py` file contains child class of the BaseController class of the [judah framework](https://github.com/sopherapps/judah/src).
      This class is responsible for controlling the data flow from the source class, through the transformers, to the destination model.  
- Each child service foldershould contain a registry of these microservices in its `__init__.py` file. The registry is just a list of the controllers of the microservices.
- The app should have a `main.py` file as the entry point of the app where the [Bonobo](https://www.bonobo-project.org/) graph is instantiated 
    and the microservice registries mentioned in the point above are added to the graph. Look at the [`example_main.py`](./example_main.py) file for inspiration.

### Why service-oriented architectural (SOA) design

Service oriented architecture makes it easy to connect actual feature requests with the actual code that is written.
Many a time, software requirements are structured in typically a service-oriented manner.
For example.
- User can see realtime data about bitcoin
- User can see realtime data about Ethereum
- User can view historical data about bitcoin

When we have source code that follows the exact manner these requirements are laid out, it is easy to comprehend for 
anyone really.

For example, for the above example, each of those requirements will have a single pipeline, each having its own
independent folder.

It is even easy to transfer that architecture into a stable microservice architecture if there is ever need to do so.

Watch [this talk by Alexandra Noonan](https://www.youtube.com/watch?v=hIFeaeZ9_AI) and [this other one by Simon Brown](https://www.youtube.com/watch?v=5OjqD-ow8GE)

## How to set up Debian server for Selenium Chrome driver

- Install an in-memory display server (xvfb)
```bash
sudo apt-get update
sudo apt-get install -y curl unzip xvfb libxi6 libgconf-2-4
```

- Install Google Chrome

```bash
sudo curl -sS -o - https://dl-ssl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
sudo echo "deb [arch=amd64]  http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list
sudo apt-get -y update
sudo apt-get -y install google-chrome-stable
```

## How to test

- Install PostgreSQL +9.5  server if you haven't already. [Here are the instructions](https://www.postgresql.org/download/)

- Clone the repo and enter its root folder

```bash
git clone https://github.com/sopherapps/judah.git && cd judah
```

- Copy the `.example.env` file to `.env` and make appropriate edits on it

```bash
cp .example.env .env
```

- Create the test database: 'test_judah' in this case

```bash
sudo -su postgres
createdb test_judah
```

- Update the `TEST_POSTGRES_DB_URI` variable in the [`.env`](.env) file to that test database's connection details

- Create a virtual environment and activate it

```bash
virtualenv -p /usr/bin/python3.6 env && source env/bin/activate
```

- Install the dependencies

```bash
pip install -r requirements.txt
```

- Run the test command

```bash
python -m unittest
```

## How to Use (Example commands for Linux)
- Ensure you have Google Chrome installed. For debian servers, see instructions under the 
title "How to set up Debian server for Selenium Chrome driver"

## Maintainers

- [Martin Ahindura](https://github.com/Tinitto)

## Folder Structure

The [`judah`](./judah) package holds the framework components that are basically base classes to be overridden.

The folder structure as generated by th command `tree -d --matchdirs -I 'env|__pycache__'` is as shown below

```
.
├── judah
│   ├── controllers
│   │   ├── base
│   │   ├── db_to_db
│   │   ├── export_site_to_db
│   │   └── rest_api_to_db
│   ├── destinations
│   │   └── database
│   ├── sources
│   │   ├── base
│   │   ├── database
│   │   ├── export_site
│   │   └── rest_api
│   ├── transformers
│   └── utils
└── test
    ├── assets
    ├── test_controllers
    ├── test_destinations
    │   └── test_database
    ├── test_sources
    │   ├── test_database
    │   ├── test_exports_site
    │   └── test_rest_api
    ├── test_transformers
    └── test_utils
```

## Acknowledgements

- The tutorial [How to Setup Selenium with ChromeDriver on Debian 10/9/8](https://tecadmin.net/setup-selenium-with-chromedriver-on-debian/) was very useful when deploying the app
on a Debian server
- The [RealPython tutorial](https://realpython.com/pypi-publish-python-package/) on publishing python packages was very helpful.
- The [Stackoverflow question about a wheel error](https://stackoverflow.com/questions/34819221/why-is-python-setup-py-saying-invalid-command-bdist-wheel-on-travis-ci) was very helpful.
