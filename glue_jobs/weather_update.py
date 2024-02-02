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
AmazonS3_node1706891145950 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://pga-tournament-data/final_hourly_dataframe_update.csv"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1706891145950",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1706901825257 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonS3_node1706891145950,
    connection_type="redshift",
    connection_options={
        "postactions": "BEGIN; MERGE INTO public.weather USING public.weather_temp_48zy5h ON weather.hourly_date = weather_temp_48zy5h.hourly_date WHEN MATCHED THEN UPDATE SET latitude = weather_temp_48zy5h.latitude, longitude = weather_temp_48zy5h.longitude, year = weather_temp_48zy5h.year, event_name = weather_temp_48zy5h.event_name, tournament_id = weather_temp_48zy5h.tournament_id, start_date = weather_temp_48zy5h.start_date, end_date = weather_temp_48zy5h.end_date, temperature_2m = weather_temp_48zy5h.temperature_2m, relative_humidity_2m = weather_temp_48zy5h.relative_humidity_2m, dew_point_2m = weather_temp_48zy5h.dew_point_2m, apparent_temperature = weather_temp_48zy5h.apparent_temperature, rain = weather_temp_48zy5h.rain, weather_code = weather_temp_48zy5h.weather_code, pressure_msl = weather_temp_48zy5h.pressure_msl, surface_pressure = weather_temp_48zy5h.surface_pressure, cloud_cover = weather_temp_48zy5h.cloud_cover, wind_speed_10m = weather_temp_48zy5h.wind_speed_10m, wind_speed_100m = weather_temp_48zy5h.wind_speed_100m, wind_direction_10m = weather_temp_48zy5h.wind_direction_10m, wind_direction_100m = weather_temp_48zy5h.wind_direction_100m, wind_gusts_10m = weather_temp_48zy5h.wind_gusts_10m, soil_temperature_0_to_7cm = weather_temp_48zy5h.soil_temperature_0_to_7cm, soil_moisture_7_to_28cm = weather_temp_48zy5h.soil_moisture_7_to_28cm, elevation = weather_temp_48zy5h.elevation, code = weather_temp_48zy5h.code, description = weather_temp_48zy5h.description, tournament_hourly = weather_temp_48zy5h.tournament_hourly, hourly_date = weather_temp_48zy5h.hourly_date WHEN NOT MATCHED THEN INSERT VALUES (weather_temp_48zy5h.latitude, weather_temp_48zy5h.longitude, weather_temp_48zy5h.year, weather_temp_48zy5h.event_name, weather_temp_48zy5h.tournament_id, weather_temp_48zy5h.start_date, weather_temp_48zy5h.end_date, weather_temp_48zy5h.temperature_2m, weather_temp_48zy5h.relative_humidity_2m, weather_temp_48zy5h.dew_point_2m, weather_temp_48zy5h.apparent_temperature, weather_temp_48zy5h.rain, weather_temp_48zy5h.weather_code, weather_temp_48zy5h.pressure_msl, weather_temp_48zy5h.surface_pressure, weather_temp_48zy5h.cloud_cover, weather_temp_48zy5h.wind_speed_10m, weather_temp_48zy5h.wind_speed_100m, weather_temp_48zy5h.wind_direction_10m, weather_temp_48zy5h.wind_direction_100m, weather_temp_48zy5h.wind_gusts_10m, weather_temp_48zy5h.soil_temperature_0_to_7cm, weather_temp_48zy5h.soil_moisture_7_to_28cm, weather_temp_48zy5h.elevation, weather_temp_48zy5h.code, weather_temp_48zy5h.description, weather_temp_48zy5h.tournament_hourly, weather_temp_48zy5h.hourly_date); DROP TABLE public.weather_temp_48zy5h; END;",
        "redshiftTmpDir": "s3://aws-glue-assets-590183762855-us-east-2/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.weather_temp_48zy5h",
        "connectionName": "Redshift connection",
        "preactions": "CREATE TABLE IF NOT EXISTS public.weather (latitude DECIMAL, longitude DECIMAL, year INTEGER, event_name VARCHAR, tournament_id VARCHAR, start_date TIMESTAMP, end_date TIMESTAMP, temperature_2m DECIMAL, relative_humidity_2m DECIMAL, dew_point_2m DECIMAL, apparent_temperature DECIMAL, rain DECIMAL, weather_code VARCHAR, pressure_msl DECIMAL, surface_pressure DECIMAL, cloud_cover DECIMAL, wind_speed_10m DECIMAL, wind_speed_100m DECIMAL, wind_direction_10m DECIMAL, wind_direction_100m DECIMAL, wind_gusts_10m DECIMAL, soil_temperature_0_to_7cm DECIMAL, soil_moisture_7_to_28cm DECIMAL, elevation VARCHAR, code VARCHAR, description VARCHAR, tournament_hourly VARCHAR, hourly_date VARCHAR); DROP TABLE IF EXISTS public.weather_temp_48zy5h; CREATE TABLE public.weather_temp_48zy5h (latitude DECIMAL, longitude DECIMAL, year INTEGER, event_name VARCHAR, tournament_id VARCHAR, start_date TIMESTAMP, end_date TIMESTAMP, temperature_2m DECIMAL, relative_humidity_2m DECIMAL, dew_point_2m DECIMAL, apparent_temperature DECIMAL, rain DECIMAL, weather_code VARCHAR, pressure_msl DECIMAL, surface_pressure DECIMAL, cloud_cover DECIMAL, wind_speed_10m DECIMAL, wind_speed_100m DECIMAL, wind_direction_10m DECIMAL, wind_direction_100m DECIMAL, wind_gusts_10m DECIMAL, soil_temperature_0_to_7cm DECIMAL, soil_moisture_7_to_28cm DECIMAL, elevation VARCHAR, code VARCHAR, description VARCHAR, tournament_hourly VARCHAR, hourly_date VARCHAR);",
    },
    transformation_ctx="AmazonRedshift_node1706901825257",
)

job.commit()
