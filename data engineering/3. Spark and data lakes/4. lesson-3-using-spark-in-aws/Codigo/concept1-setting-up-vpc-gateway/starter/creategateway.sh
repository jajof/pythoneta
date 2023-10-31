#!/bin/bash

# Create the S3 Gateway, replacing the blanks with the VPC and Routing Table Ids: aws ec2 create-vpc-endpoint --vpc-id _______ --service-name com.amazonaws.region.s3 \ --route-table-ids _______
aws ec2 create-vpc-endpoint --vpc-id vpc-0c12260f3a31dfe30 --service-name com.amazonaws.us-east-1.s3 --route-table-ids rtb-083cc4ec0bc9d7433

