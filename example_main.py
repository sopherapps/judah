"""
import os

import bonobo
import logging

from dotenv import load_dotenv

from judah.utils.assets import get_asset_path
from judah.utils.logging import setup_rotating_file_logger

load_dotenv()

# Assuming you have an rte ETL service, a rest_api_to_db child service and a number of microservices
# each corresponding to a given dataset
from app.services.rte.rest_api_to_db import RTE_REST_API_TO_DB_CONTROLLERS


def get_graph(**options):
    '''
    This function builds the graph that needs to be executed.
    :return: bonobo.Graph
    '''
    graph = bonobo.Graph()

    for controller in RTE_REST_API_TO_DB_CONTROLLERS:
        graph.add_chain(
            controller.extract,
            controller.transform,
            controller.load,
        )

    return graph


def get_services(**options):
    '''Service Dependency injector'''
    return {}


if __name__ == '__main__':
    logger = logging.getLogger()
    setup_rotating_file_logger(file_path=get_asset_path('error.log'), logger=logger)

    logging.disable(getattr(logging, os.getenv('LOGGING_LEVEL_DISABLE', 'NOTSET')))

    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
"""
