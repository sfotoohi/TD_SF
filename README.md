# Queries

Here are the scenarios that I created to verify different outcomes

####Using the hive query engine outputting to CSV

sample_datasets www_access path,host,agent 1412338845 1412339246  hive  csv
<br>Result: <b>https://github.com/sfotoohi/TD_SF/blob/master/output1</b>

####Using the presto query engine outputting to screen in a tabular format

sample_datasets www_access path,host,agent 1412338845 1412339246  presto  tabular
<br>Result: <b>https://github.com/sfotoohi/TD_SF/blob/master/output2</b>

####Providing no max_time

sample_datasets www_access path,host,agent 1412338845 NULL  presto  tabular
<br>Result: <b>https://github.com/sfotoohi/TD_SF/blob/master/output3</b>

####Providing no min_time

sample_datasets www_access path,host,agent NULL 1412339246  presto  tabular
<br>Result: <b>https://github.com/sfotoohi/TD_SF/blob/master/output4</b>

####Providing not min_time and no max_time resulting in pulling all records

sample_datasets www_access path,host,agent NULL NULL  presto  tabular
<br>Result: <b>https://github.com/sfotoohi/TD_SF/blob/master/output5</b>

