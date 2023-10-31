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

# Script generated for node customer_curated
customer_curated_node1698699518302 = glueContext.create_dynamic_frame.from_catalog(
    database="jhoffmann-datalake",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1698699518302",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1698699518846 = glueContext.create_dynamic_frame.from_catalog(
    database="jhoffmann-datalake",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_node1698699518846",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1698723301788 = ApplyMapping.apply(
    frame=step_trainer_landing_node1698699518846,
    mappings=[
        ("sensorreadingtime", "long", "sensorreadingtime", "long"),
        ("serialnumber", "string", "serialnumber_y", "string"),
        ("distancefromobject", "int", "distancefromobject", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1698723301788",
)

# Script generated for node Join
Join_node1698720634289 = Join.apply(
    frame1=customer_curated_node1698699518302,
    frame2=RenamedkeysforJoin_node1698723301788,
    keys1=["serialnumber"],
    keys2=["serialnumber_y"],
    transformation_ctx="Join_node1698720634289",
)

# Script generated for node Change Schema
ChangeSchema_node1698721983108 = ApplyMapping.apply(
    frame=Join_node1698720634289,
    mappings=[
        ("sensorreadingtime", "long", "sensorreadingtime", "long"),
        ("serialnumber_y", "string", "serialnumber", "string"),
        ("distancefromobject", "int", "distancefromobject", "int"),
    ],
    transformation_ctx="ChangeSchema_node1698721983108",
)

# Script generated for node Amazon S3
AmazonS3_node1698720689107 = glueContext.getSink(
    path="s3://jh-data-lakehouse/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1698720689107",
)
AmazonS3_node1698720689107.setCatalogInfo(
    catalogDatabase="jhoffmann-datalake", catalogTableName="step_trainer_trusted"
)
AmazonS3_node1698720689107.setFormat("json")
AmazonS3_node1698720689107.writeFrame(ChangeSchema_node1698721983108)
job.commit()
