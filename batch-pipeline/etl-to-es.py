#!/usr/bin/env python

from argparse import ArgumentParser
import json
import logging
import os

import findspark

from pyspark.sql import SparkSession, SQLContext, types

logging.basicConfig(level=logging.WARNING)
STACK_OVERFLOW_SCHEMA = "stackoverflow-schema.json"
ES_INDEX = "so-questions"


def main():
    parser = ArgumentParser("Bulk insert stack overflow questions into elastic search")
    parser.add_argument("file", help="Regex corresponding to files in s3.")
    parser.add_argument(
        "--mapping", help="Path to mapping.json.", default=STACK_OVERFLOW_SCHEMA
    )
    parser.add_argument("--s3-url", help="S3 URL prefix", default=os.getenv("S3_URL"))
    parser.add_argument("--es-index", help="Index in ES.", default=ES_INDEX)
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
    count = df.count()
    logging.info(f"ETL-ing {count} documents to elastic search.")
    df.write.format("org.elasticsearch.spark.sql").option(
        "es.nodes", os.getenv("ES_URL")
    ).option("es.port", 443).option("es.resource", args.es_index).option(
        "es.nodes.wan.only", True
    ).mode(
        "append"
    ).save()

    logging.info("Completed ETL-ing data into elastic search.")


if __name__ == "__main__":
    main()
