#!/bin/bash
#
# First we must configure the credentials in airflow Admin Connections
#
# TO-DO: run the follwing command and observe the JSON output: 
# airflow connections get aws_credentials -o json 
# 

# [{"id": "67", 
# "conn_id": "aws_credentials", 
# "conn_type": "aws", 
# "description": "", 
# "host": "", 
# "schema": "", 
# "login": "AKIASVOIERB3XS2UTDWN", 
# "password": "wi8OQDVoMHzo07YJ8r3vjqy5CYOScHDGLrGLMR47", 
# "port": null, 
# "is_encrypted": "True", 
# "is_extra_encrypted": "True", 
# "extra_dejson": {}, 
# "get_uri": "aws://AKIASVOIERB3XS2UTDWN:wi8OQDVoMHzo07YJ8r3vjqy5CYOScHDGLrGLMR47@"
# }]
#
# Copy the value after "get_uri":
#
# For example: aws://AKIASVOIERB3XS2UTDWN:wi8OQDVoMHzo07YJ8r3vjqy5CYOScHDGLrGLMR47@
#
# TO-DO: Update the following command with the URI and un-comment it:
#
airflow connections add aws_credentials --conn-uri 'aws://AKIASVOIERB3XS2UTDWN:wi8OQDVoMHzo07YJ8r3vjqy5CYOScHDGLrGLMR47@'
#
# After we generated the credentials in connections
# TO-DO: run the follwing command and observe the JSON output: 
# airflow connections get redshift -o json
# 
# [{"id": "68", 
# "conn_id": "redshift", 
# "conn_type": "redshift", 
# "description": "", 
# "host": "default-workgroup.183492708471.us-east-1.redshift-serverless.amazonaws.com", 
# "schema": "dev", 
# "login": "awsuser", 
# "password": "Passw0rd", 
# "port": "5439", 
# "is_encrypted": "True", 
# "is_extra_encrypted": "True", 
# "extra_dejson": {}, 
# "get_uri": "redshift://awsuser:Passw0rd@default-workgroup.183492708471.us-east-1.redshift-serverless.amazonaws.com:5439/dev"}]
#
# Copy the value after "get_uri":
#
# For example: redshift://awsuser:Passw0rd@default-workgroup.183492708471.us-east-1.redshift-serverless.amazonaws.com:5439/dev
#
# TO-DO: Update the following command with the URI and un-comment it:
#
airflow connections add redshift --conn-uri 'redshift://awsuser:Passw0rd@default-workgroup.183492708471.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
#
# TO-DO: update the following bucket name to match the name of your S3 bucket and un-comment it:
#
airflow variables set s3_bucket jh-bucket
#
# TO-DO: un-comment the below line:
#
airflow variables set s3_prefix data-pipelines