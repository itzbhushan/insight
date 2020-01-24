#!/usr/bin/env python

from argparse import ArgumentParser
import json
import os

# from clio_lite import clio_search
from elasticsearch import Elasticsearch


def main():
    parser = ArgumentParser("Bulk insert subreddit submissions into elastic search")
    parser.add_argument("subreddit", type=str.lower, help="Subreddit name.")
    args = parser.parse_args()
    es_url = os.getenv("ES_URL")
    es = Elasticsearch(es_url)

    index = "-".join([args.subreddit, "submissions"])
    # total, docs = clio_search(es_url, index=index, query="shotgun")
    # search_query = {"query": { "match": { "title": "shotgun stop" } } }
    search_query = {"query": {"match": {"title": "python pandas"}}}
    result = es.search(index=index, body=search_query)
    print(result)


if __name__ == "__main__":
    main()
