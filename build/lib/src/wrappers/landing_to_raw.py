"""raw layer wrapper"""

import sys
from src.ingestion.landing_to_raw_ingestion import LandingToRawIngestion
from src.utils.env_args import RawArgs


def lambda_handler(event, context):
    """main method"""
    params = RawArgs(event)
    ob = LandingToRawIngestion(params)
    ob.build()

