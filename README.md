# Instructions

Here are the scenarios that I created to verify different outcomes

Using the hive query engine outputting to CSV

sample_datasets www_access path,host,agent 1412338845 1412339246  hive  csv

Using the presto query engine outputting to screen in a tabular format

sample_datasets www_access path,host,agent 1412338845 1412339246  presto  tabular

Providing no max_time

sample_datasets www_access path,host,agent 1412338845 NULL  presto  tabular

Providing no min_time

sample_datasets www_access path,host,agent NULL 1412339246  presto  tabular

Providing not min_time and no max_time resulting in pulling all records

sample_datasets www_access path,host,agent NULL NULL  presto  tabular

