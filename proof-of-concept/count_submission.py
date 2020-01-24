#!/usr/bin/env python

import json

from collections import defaultdict

import pandas as pd


file_name = "/mnt/RS_2017-01"
counter = defaultdict(int)

x = 0
with open(file_name) as f:
    for line in f:
        l_json = json.loads(line)
        sub_reddit = l_json.get("subreddit", None)
        counter[sub_reddit] += 1
        if x > 100_000_000:
            break
        x += 1


sorted_count = [
    [k, v] for k, v in sorted(counter.items(), key=lambda item: item[1], reverse=True)
]

print(sorted_count[:10])
