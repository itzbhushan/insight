#!/usr/bin/env python

from argparse import ArgumentParser
import json
import os

from elasticsearch import Elasticsearch
from elasticsearch import helpers


def extract_documents(subdreddit, file_path, index):
    columns = ["title", "url", "created_utc"]
    with open(file_path) as f:
        for line in f:
            l_json = json.loads(line)
            if l_json.get("subreddit", "").lower() != subdreddit:
                continue

            doc = {"_index": index}
            doc["_source"] = {k: l_json.get(k) for k in columns}
            yield doc


def main():
    parser = ArgumentParser("Bulk insert subreddit submissions into elastic search")
    parser.add_argument("subreddit", type=str.lower, help="Subreddit name.")
    parser.add_argument("path", help="Path to raw file.")
    args = parser.parse_args()
    es_url = os.getenv("ES_URL")
    es = Elasticsearch(es_url)

    index = "-".join([args.subreddit, "submissions"])
    es.indices.create(
        index,
        {
            "mappings": {
                "properties": {
                    "title": {"type": "text"},
                    "url": {"type": "text"},
                    "created_utc": {"type": "date"},
                }
            }
        },
    )

    docs = extract_documents(args.subreddit, args.path, index)
    helpers.bulk(es, docs)


if __name__ == "__main__":
    main()
