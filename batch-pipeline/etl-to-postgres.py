#!/usr/bin/env python

from argparse import ArgumentParser
import json
import logging
import os

import findspark

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession, SQLContext, functions as func

logging.basicConfig(level=logging.WARNING, datefmt="%Y-%m-%d %H:%M:%S")
POSTGRES_PREFIX = "jdbc:postgresql://"


def etl_into_questions_table(df, db_url, credentials):
    """
    ETL questions metadata into postgresl.

    This function extracts metadata about questions (columns like id,
    creation_date, score, user_id, site, and answer_count) and loads
    them to the "questions" table. It is assumed that the table has
    already been created.
    """

    columns = (
        "id",
        "creation_date",
        "score",
        "owner.user_id",
        "site",
        "answer_count",
        "link",
    )
    table = "questions"
    question_table_df = df.select(*columns)
    question_table_df = question_table_df.withColumn(
        "creation_date", df["creation_date"].cast("timestamp")
    )
    try:
        question_table_df.write.jdbc(
            url=db_url, table=table, mode="append", properties=credentials
        )
    except Py4JJavaError:
        logging.exception("Unable to ETL into questions table.")
        return 1


def etl_into_users_table(df, db_url, credentials):
    """
    ETL users metadata into postgresql.

    This function extracts metadata about users (like user_id, reputation
    and site). Null user_ids are excluded and the resulting data frame is
    transformed to get the latest reputation of the user (based on max
    reputation) and loaded into "users" table. It is assumed that the table
    has already been created.
    """

    columns = ("owner.user_id", "owner.reputation", "site")
    table = "users"
    user_table_df = df.select(*columns)
    non_null_users = user_table_df.na.drop(subset=["user_id"])

    # user information may be repeated in questions. However, the user
    # table contains only one entry per user. If user x repeats twice,
    # we need to find the way to store the latest information about
    # that user. For this project, we make an assumption that the max
    # reputation represents the latest information and only store that
    # reputation in the dataase. In the real world, as reputations change,
    # the table will be updated directly.
    latest_user_info = non_null_users.groupby("site", "user_id").agg(
        func.max("reputation").alias("reputation")
    )

    try:
        latest_user_info.write.jdbc(
            url=db_url, table=table, mode="append", properties=credentials
        )
    except Py4JJavaError:
        logging.exception("Unable to ETL into users table.")
        return 1


def main():
    parser = ArgumentParser("ETL stack overflow questions metadata to postgres.")
    parser.add_argument("file", help="Regex corresponding to files in s3.")
    parser.add_argument("--s3-url", help="S3 URL prefix", default=os.getenv("S3_URL"))
    parser.add_argument(
        "--postgres-url", help="PostgreSQL Endpoint", default=os.getenv("POSTGRES_URL")
    )
    parser.add_argument("--postgres-db", help="PostgreSQL database", default="postgres")
    args = parser.parse_args()

    spark_db_url = POSTGRES_PREFIX + os.path.join(args.postgres_url, args.postgres_db)
    db_credentials = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PWD"),
    }

    findspark.init()
    spark = SparkSession.builder.appName("etl-to-postgres").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)

    logging.info("Starting ETL from S3 to postgres.")

    data_file = os.path.join(args.s3_url, args.file)
    df = sqlContext.read.json(data_file)
    df = df.filter(df.type == "question")

    logging.info("S3 read complete. Writing to postgres")

    fail_status = etl_into_questions_table(df, spark_db_url, db_credentials)
    if not fail_status:  # success
        fail_status = etl_into_users_table(df, spark_db_url, db_credentials)

    if not fail_status:
        logging.info("ETL into postgres is complete.")
    else:
        logging.error("ETL failed.")

    return fail_status


if __name__ == "__main__":
    main()
