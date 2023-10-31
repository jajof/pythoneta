import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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

# Script generated for node accelerometer_landing
accelerometer_landing_node1698699518846 = glueContext.create_dynamic_frame.from_catalog(
    database="jhoffmann-datalake",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node1698699518846",
)

# Script generated for node Join
Join_node1698720634289 = Join.apply(
    frame1=customer_trusted_node1698699518302,
    frame2=accelerometer_landing_node1698699518846,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1698720634289",
)

# Script generated for node Change Schema
ChangeSchema_node1698721983108 = ApplyMapping.apply(
    frame=Join_node1698720634289,
    mappings=[
        ("user", "string", "user", "string"),
        ("timestamp", "long", "timestamp", "long"),
        ("x", "float", "x", "float"),
        ("y", "float", "y", "float"),
        ("z", "float", "z", "float"),
    ],
    transformation_ctx="ChangeSchema_node1698721983108",
)

# Script generated for node Amazon S3
AmazonS3_node1698720689107 = glueContext.getSink(
    path="s3://jh-data-lakehouse/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1698720689107",
)
AmazonS3_node1698720689107.setCatalogInfo(
    catalogDatabase="jhoffmann-datalake", catalogTableName="accelerometer_trusted"
)
AmazonS3_node1698720689107.setFormat("json")
AmazonS3_node1698720689107.writeFrame(ChangeSchema_node1698721983108)
job.commit()
