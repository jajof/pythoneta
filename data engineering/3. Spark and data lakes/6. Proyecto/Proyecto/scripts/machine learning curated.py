import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step_trainer
step_trainer_node1698724519821 = glueContext.create_dynamic_frame.from_catalog(
    database="jhoffmann-datalake",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_node1698724519821",
)

# Script generated for node accelerometer
accelerometer_node1698724521427 = glueContext.create_dynamic_frame.from_catalog(
    database="jhoffmann-datalake",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_node1698724521427",
)

# Script generated for node customer
customer_node1698724522081 = glueContext.create_dynamic_frame.from_catalog(
    database="jhoffmann-datalake",
    table_name="customer_curated",
    transformation_ctx="customer_node1698724522081",
)

# Script generated for node SQL Query
SqlQuery813 = """
Select 
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject,
    a.timestamp,
    a.x,
    a.y,
    a.z
from 
    step_trainer s
    inner join accelerometer a on a.timestamp = s.sensorreadingtime
    inner join customer c on c.email  = a.user
--where c.sharewithresearchasofdate < s.sensorreadingtime
"""
SQLQuery_node1698725401618 = sparkSqlQuery(
    glueContext,
    query=SqlQuery813,
    mapping={
        "step_trainer": step_trainer_node1698724519821,
        "accelerometer": accelerometer_node1698724521427,
        "customer": customer_node1698724522081,
    },
    transformation_ctx="SQLQuery_node1698725401618",
)

# Script generated for node Amazon S3
AmazonS3_node1698725753297 = glueContext.getSink(
    path="s3://jh-data-lakehouse/machine-learning/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1698725753297",
)
AmazonS3_node1698725753297.setCatalogInfo(
    catalogDatabase="jhoffmann-datalake", catalogTableName="machine_learning_curated"
)
AmazonS3_node1698725753297.setFormat("json")
AmazonS3_node1698725753297.writeFrame(SQLQuery_node1698725401618)
job.commit()
