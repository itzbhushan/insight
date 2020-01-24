#!/usr/bin/env python

from argparse import ArgumentParser
import json
import os

# from clio_lite import clio_search
from elasticsearch import Elasticsearch


def main():
    parser = ArgumentParser("Query elastic search.")
    parser.add_argument("subreddit", type=str.lower, help="Subreddit name.")
    parser.add_argument("text", type=str.lower, help="Text to search.")
    args = parser.parse_args()
    es_url = os.getenv("ES_URL")
    es = Elasticsearch(es_url)

    index = "-".join([args.subreddit, "submissions"])
    # total, docs = clio_search(es_url, index=index, query="shotgun")
    search_query = {"query": {"match": {"title": args.text}}}
    response = es.search(index=index, body=search_query)
    print("Found {} matches".format(len(response["hits"]["hits"])))
    for result in response["hits"]["hits"]:
        print(
            "Title: {}, score: {}".format(result["_source"]["title"], result["_score"])
        )


if __name__ == "__main__":
    main()
