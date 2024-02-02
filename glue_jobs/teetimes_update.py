import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1706894097485 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://pga-tournament-data/df_tt_updated.csv"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1706894097485",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1706898667690 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonS3_node1706894097485,
    connection_type="redshift",
    connection_options={
        "postactions": "BEGIN; MERGE INTO public.tee_times USING public.tee_times_temp_zgvf1w ON tee_times.ttid_playerid = tee_times_temp_zgvf1w.ttid_playerid WHEN MATCHED THEN UPDATE SET tournament_id = tee_times_temp_zgvf1w.tournament_id, tt_id = tee_times_temp_zgvf1w.tt_id, roundStatus = tee_times_temp_zgvf1w.roundStatus, roundInt = tee_times_temp_zgvf1w.roundInt, roundDisplay = tee_times_temp_zgvf1w.roundDisplay, roundStatusColor = tee_times_temp_zgvf1w.roundStatusColor, groupHole = tee_times_temp_zgvf1w.groupHole, groupLocation = tee_times_temp_zgvf1w.groupLocation, holeLocation = tee_times_temp_zgvf1w.holeLocation, groupNumber = tee_times_temp_zgvf1w.groupNumber, groupStatus = tee_times_temp_zgvf1w.groupStatus, startTee = tee_times_temp_zgvf1w.startTee, teeTime_utc = tee_times_temp_zgvf1w.teeTime_utc, courseId = tee_times_temp_zgvf1w.courseId, playerLastName = tee_times_temp_zgvf1w.playerLastName, playerId = tee_times_temp_zgvf1w.playerId, latitude = tee_times_temp_zgvf1w.latitude, longitude = tee_times_temp_zgvf1w.longitude, local_time = tee_times_temp_zgvf1w.local_time, local_time_format = tee_times_temp_zgvf1w.local_time_format, ttid_playerid = tee_times_temp_zgvf1w.ttid_playerid WHEN NOT MATCHED THEN INSERT VALUES (tee_times_temp_zgvf1w.tournament_id, tee_times_temp_zgvf1w.tt_id, tee_times_temp_zgvf1w.roundStatus, tee_times_temp_zgvf1w.roundInt, tee_times_temp_zgvf1w.roundDisplay, tee_times_temp_zgvf1w.roundStatusColor, tee_times_temp_zgvf1w.groupHole, tee_times_temp_zgvf1w.groupLocation, tee_times_temp_zgvf1w.holeLocation, tee_times_temp_zgvf1w.groupNumber, tee_times_temp_zgvf1w.groupStatus, tee_times_temp_zgvf1w.startTee, tee_times_temp_zgvf1w.teeTime_utc, tee_times_temp_zgvf1w.courseId, tee_times_temp_zgvf1w.playerLastName, tee_times_temp_zgvf1w.playerId, tee_times_temp_zgvf1w.latitude, tee_times_temp_zgvf1w.longitude, tee_times_temp_zgvf1w.local_time, tee_times_temp_zgvf1w.local_time_format, tee_times_temp_zgvf1w.ttid_playerid); DROP TABLE public.tee_times_temp_zgvf1w; END;",
        "redshiftTmpDir": "s3://aws-glue-assets-590183762855-us-east-2/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.tee_times_temp_zgvf1w",
        "connectionName": "Redshift connection",
        "preactions": "CREATE TABLE IF NOT EXISTS public.tee_times (tournament_id VARCHAR, tt_id VARCHAR, roundStatus VARCHAR, roundInt INTEGER, roundDisplay VARCHAR, roundStatusColor VARCHAR, groupHole VARCHAR, groupLocation VARCHAR, holeLocation VARCHAR, groupNumber VARCHAR, groupStatus VARCHAR, startTee VARCHAR, teeTime_utc TIMESTAMP, courseId VARCHAR, playerLastName VARCHAR, playerId VARCHAR, latitude DECIMAL, longitude DECIMAL, local_time TIMESTAMP, local_time_format VARCHAR, ttid_playerid VARCHAR); DROP TABLE IF EXISTS public.tee_times_temp_zgvf1w; CREATE TABLE public.tee_times_temp_zgvf1w (tournament_id VARCHAR, tt_id VARCHAR, roundStatus VARCHAR, roundInt INTEGER, roundDisplay VARCHAR, roundStatusColor VARCHAR, groupHole VARCHAR, groupLocation VARCHAR, holeLocation VARCHAR, groupNumber VARCHAR, groupStatus VARCHAR, startTee VARCHAR, teeTime_utc TIMESTAMP, courseId VARCHAR, playerLastName VARCHAR, playerId VARCHAR, latitude DECIMAL, longitude DECIMAL, local_time TIMESTAMP, local_time_format VARCHAR, ttid_playerid VARCHAR);",
    },
    transformation_ctx="AmazonRedshift_node1706898667690",
)

job.commit()
