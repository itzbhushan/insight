#!/usr/bin/env python

from argparse import ArgumentParser
import json
import logging
import os
import sys

import findspark

from pyspark.sql import SparkSession, SQLContext, types
from pyspark.sql.functions import col, when

logging.basicConfig(level=logging.WARNING)
STACK_OVERFLOW_SCHEMA = "stackoverflow-schema.json"
ES_INDEX = "so-questions"


index_mapping_types = ["default", "custom"]


def main():
    parser = ArgumentParser("Bulk insert stack overflow questions into elastic search")
    parser.add_argument("file", help="Regex corresponding to files in s3.")
    parser.add_argument(
        "--mapping", help="Path to mapping.json.", default=STACK_OVERFLOW_SCHEMA
    )
    parser.add_argument("--s3-url", help="S3 URL prefix.", default=os.getenv("S3_URL"))
    parser.add_argument(
        "--index-type",
        help="Type of index.",
        default="default",
        choices=index_mapping_types,
    )
    parser.add_argument(
        "--default-index",
        help="If index type is default, then the name of the default index.",
        default=ES_INDEX,
    )
    parser.add_argument("--custom-index", nargs=2)
    args = parser.parse_args()

    with open(args.mapping) as f:
        schema = json.loads(f.read())["mappings"]["properties"]
        columns = schema.keys()

    findspark.init()
    spark = SparkSession.builder.appName("write-to-elastic-search").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)

    logging.info("Reading data from S3 and writing to elastic search in progress.")

    data_file = os.path.join(args.s3_url, args.file)
    df = sqlContext.read.json(data_file)

    # Raw file contains close to a hundred columns (or keys). We
    # only select a handful of them (columns saved in args.mapping)
    # and index them in elastic search.
    df = df.filter(df.type == "question").select(*columns)
    logging.info(f"ETL-ing {df.count()} documents to elastic search.")

    if args.index_type == "default":
        index = args.default_index
    else:
        index_a, index_b = args.custom_index
        df = df.withColumn(
            "index", when(col("site") == "stackoverflow", index_a).otherwise(index_b)
        )
        index = "{index}"  # use the index column in dataframe as the index.

    df.write.format("org.elasticsearch.spark.sql").option(
        "es.nodes", os.getenv("ES_URL")
    ).option("es.port", 443).option("es.resource", index).option(
        "es.nodes.wan.only", True
    ).mode(
        "overwrite"
    ).save()

    logging.info("Completed ETL-ing data into elastic search.")


if __name__ == "__main__":
    main()
