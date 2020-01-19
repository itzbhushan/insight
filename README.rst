***************
you-complete-me
***************

Introduction
############

*you-complete-me* is a real time text analytics data pipeline for providing suggestions based on
semantic information to users posting questions in social media platforms.

Business use case
#################

Providing relevant suggestions in real time can improve platform engagement, improve content
quality and content discovery. Near real time suggestions can also improve customer satisfaction
when interacting with chatbots.

Existing Solutions
##################
Several platforms like Google, Stack Overflow provide auto complete based suggestions.
However, such suggestions are mainly based on prefix based string compare (using trie
or similar data structure). Instead, my proposal uses semantic information in the text
and hence can provide much richer suggestions than conventional prefix based solutions.
In general, both the proposal and the existing solutions can be combined to
to improve the quality of suggestions.

Engineering Challenge
#####################

Minimizing latency and scaling resources with increasing dataset size or users are the primary
engineering challenges.

Tech stack
##########

This project has pipelines for both batch and stream processing with more emphasis on the streaming pipeline.
For the batch pipeline, I use S3 -> Spark -> Elastic Search, while the streaming pipeline uses
local-application <-> web-server <-> Pulsar <-> Elastic-search.

Data source
###########

I will be using a subset of the reddit_ dataset for this project.

.. _reddit: https://files.pushshift.io/reddit/

Minimum-viable Project (MVP)
############################

An MVP version of the product would include the batch and streaming pipeline that can provide suggestion
for atleast one user.
