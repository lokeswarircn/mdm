"""trusted layer wrapper"""

import sys
from awsglue.context import GlueContext
from pyspark import SparkContext
from src.utils.params import Params
from src.ingestion.raw_to_trusted_ingestion import RawToTrustedIngestion
from src.utils.env_args import TrustedArgs
from src.utils.logger_builder import LoggerBuilder

log = LoggerBuilder().build()

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


def main():
    """main method"""
    log.info("raw to trusted job started")
    params = TrustedArgs(Params(sys.argv))
    ob = RawToTrustedIngestion(params, spark)
    ob.build()


if __name__ == "__main__":
    main()
