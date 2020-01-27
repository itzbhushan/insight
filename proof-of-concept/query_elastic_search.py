#!/usr/bin/env python

from argparse import ArgumentParser
import json
import os

# from clio_lite import clio_search
from elasticsearch import Elasticsearch


def search(es, index, query):
    response = es.search(index=index, body=query)
    print(
        f"Found {response['hits']['total']['value']} matches. Took {response['took']} ms."
    )
    for result in response["hits"]["hits"]:
        print(
            "Title: {}, score: {}".format(result["_source"]["title"], result["_score"])
        )
    return response


def main():
    parser = ArgumentParser("Query elastic search.")
    parser.add_argument("subreddit", type=str.lower, help="Subreddit name.")
    parser.add_argument("text", type=str.lower, help="Text to search.")
    args = parser.parse_args()
    es_url = os.getenv("ES_URL")
    es = Elasticsearch(es_url)

    index = "-".join([args.subreddit, "submissions"])
    # total, docs = clio_search(es_url, index=index, query="shotgun")
    limit = 5
    query = {"query": {"match": {"selftext": args.text}}, "size": limit}
    _ = search(es, index, query)

    query = {
        "query": {
            "more_like_this": {
                "fields": ["selftext", "title"],
                "like": args.text,
                "min_term_freq": 1,
                "max_query_terms": 20,
            }
        }
    }
    response = search(es, index, query)


if __name__ == "__main__":
    main()
