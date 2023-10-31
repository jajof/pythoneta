import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_trusted
customer_trusted_node1698699518302 = glueContext.create_dynamic_frame.from_catalog(
    database="jhoffmann-datalake",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1698699518302",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1698699518846 = glueContext.create_dynamic_frame.from_catalog(
    database="jhoffmann-datalake",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1698699518846",
)

# Script generated for node Join
Join_node1698720634289 = Join.apply(
    frame1=customer_trusted_node1698699518302,
    frame2=accelerometer_trusted_node1698699518846,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1698720634289",
)

# Script generated for node Change Schema
ChangeSchema_node1698721983108 = ApplyMapping.apply(
    frame=Join_node1698720634289,
    mappings=[
        ("customername", "string", "customername", "string"),
        ("email", "string", "email", "string"),
        ("phone", "string", "phone", "string"),
        ("birthday", "string", "birthday", "string"),
        ("serialnumber", "string", "serialnumber", "string"),
        ("registrationdate", "long", "registrationdate", "long"),
        ("lastupdatedate", "long", "lastupdatedate", "long"),
        ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"),
        ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"),
        ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long"),
    ],
    transformation_ctx="ChangeSchema_node1698721983108",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1698722801394 = DynamicFrame.fromDF(
    ChangeSchema_node1698721983108.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1698722801394",
)

# Script generated for node Amazon S3
AmazonS3_node1698720689107 = glueContext.getSink(
    path="s3://jh-data-lakehouse/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1698720689107",
)
AmazonS3_node1698720689107.setCatalogInfo(
    catalogDatabase="jhoffmann-datalake", catalogTableName="customer_curated"
)
AmazonS3_node1698720689107.setFormat("json")
AmazonS3_node1698720689107.writeFrame(DropDuplicates_node1698722801394)
job.commit()
