#!/usr/bin/env python

from argparse import ArgumentParser, FileType
import json
import os

from elasticsearch import Elasticsearch
from elasticsearch import helpers


# Data cleaning: Consolidate rules to blacklist in a single function.
def blacklist_message(msg, subreddit):
    if msg.get("subreddit", "").lower() != subreddit:
        return True

    # if the body of a message is less than 20 characters, ignore it.
    # We assume that such messages are not really meaningful.
    blacklist_selftext_len = 20
    if len(msg.get("selftext", "")) < blacklist_selftext_len:
        return True


def extract_documents(subreddit, file_path, index, columns):
    with open(file_path) as f:
        for line in f:
            l_json = json.loads(line)
            if blacklist_message(l_json, subreddit):
                continue

            doc = {"_index": index, "_id": l_json["id"]}
            doc.update({k: l_json.get(k) for k in columns})
            yield doc


def main():
    parser = ArgumentParser("Bulk insert subreddit submissions into elastic search")
    parser.add_argument("subreddit", type=str.lower, help="Subreddit name.")
    parser.add_argument("path", help="Path to raw file.")
    parser.add_argument("mapping", help="Path to mapping.json", type=FileType("r"))
    args = parser.parse_args()
    es_url = os.getenv("ES_URL")
    es = Elasticsearch(es_url)

    index = "-".join([args.subreddit, "submissions"])
    mappings = json.loads(args.mapping.read())

    # Error 400 indicates that index already exists. In this,
    # just ignore and do not create the index.
    es.indices.create(index, mappings, ignore=400)
    columns = mappings["mappings"]["properties"].keys()

    docs = extract_documents(args.subreddit, args.path, index, columns)
    helpers.bulk(es, docs)


if __name__ == "__main__":
    main()
