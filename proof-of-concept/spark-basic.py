#!/usr/bin/env python

from argparse import ArgumentParser
import json
import logging
import os

import findspark

from pyspark.sql import SparkSession, SQLContext, types
from pyspark.sql import functions as func

logging.basicConfig(level=logging.WARNING)


def main():
    parser = ArgumentParser("Bulk insert stack overflow questions into elastic search")
    parser.add_argument(
        "--file", help="Regex corresponding to files.", default="STX_2017-01"
    )
    parser.add_argument("--url", help="S3 URL prefix", default=os.getenv("S3_URL"))
    args = parser.parse_args()

    findspark.init()
    spark = SparkSession.builder.appName("spark-basic").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)

    data_file = os.path.join(args.url, args.file)
    df = sqlContext.read.json(data_file)
    # convert creation_date bigint into timestamp.
    df = df.withColumn("creation_ts", df["creation_date"].cast("timestamp"))
    # extract date from timestamp
    df = df.withColumn("date", df["creation_ts"].cast("date"))
    # extract hour info from the timestamp
    df = df.withColumn("hour", func.hour("creation_ts"))

    print("Number of messages per day")
    count_df = df.groupby(df.date).count()
    count_df.show()
    print("Number of messages per hour (across the month)")
    count_df = df.groupby(df.hour).count()
    count_df.show()


if __name__ == "__main__":
    main()
