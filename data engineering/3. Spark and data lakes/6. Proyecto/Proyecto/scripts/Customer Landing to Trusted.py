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

# Script generated for node Amazon S3
AmazonS3_node1698471256636 = glueContext.create_dynamic_frame.from_catalog(
    database="jhoffmann-datalake",
    table_name="customer_landing",
    transformation_ctx="AmazonS3_node1698471256636",
)

# Script generated for node SQL Query
SqlQuery709 = """
select * from myDataSource
where shareWithResearchAsOfDate <> 0


"""
SQLQuery_node1698697682178 = sparkSqlQuery(
    glueContext,
    query=SqlQuery709,
    mapping={"myDataSource": AmazonS3_node1698471256636},
    transformation_ctx="SQLQuery_node1698697682178",
)

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node1698471621299 = glueContext.getSink(
    path="s3://jh-data-lakehouse/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="TrustedCustomerZone_node1698471621299",
)
TrustedCustomerZone_node1698471621299.setCatalogInfo(
    catalogDatabase="jhoffmann-datalake", catalogTableName="customer_trusted"
)
TrustedCustomerZone_node1698471621299.setFormat("json")
TrustedCustomerZone_node1698471621299.writeFrame(SQLQuery_node1698697682178)
job.commit()
