#!/usr/bin/env python

"""
Test script to produce and send messages to the broker. The
consumer was simulated using the following command:
    pulsar-client consume my-topic -n 10 -s consumer-test
"""
import pulsar

client = pulsar.Client(
    "pulsar://ec2-54-212-169-175.us-west-2.compute.amazonaws.com:6650"
)

producer = client.create_producer("my-topic")

for i in range(10):
    producer.send(("Hello-%d" % i).encode("utf-8"))

client.close()
