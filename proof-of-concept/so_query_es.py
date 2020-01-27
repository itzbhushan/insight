#!/usr/bin/env python

from argparse import ArgumentParser
import json
import os

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
    parser.add_argument("text", type=str.lower, help="Text to search.")
    args = parser.parse_args()
    es_url = os.getenv("ES_URL")
    es = Elasticsearch(es_url)

    index = "so-questions"
    limit = 5
    query = {"query": {"match": {"body": args.text}}, "size": limit}
    _ = search(es, index, query)

    query = {
        "query": {
            "more_like_this": {
                "fields": ["body", "title"],
                "like": args.text,
                "min_term_freq": 1,
                "max_query_terms": 20,
            }
        }
    }
    response = search(es, index, query)


if __name__ == "__main__":
    main()
