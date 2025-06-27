"""enriched layer wrapper"""

import sys
from awsglue.context import GlueContext
from pyspark import SparkContext
from src.utils.params import Params
from src.ingestion.trusted_to_enriched import TrustedToEnrichedIngestion
from src.utils.env_args import EnrichedArgs

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


def main():
    """main method"""
    params = EnrichedArgs(Params(sys.argv))
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    ob = TrustedToEnrichedIngestion(params, spark)
    ob.build()


if __name__ == "__main__":
    main()
