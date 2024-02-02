import boto3
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import openmeteo_requests
import requests

# Global variables/constants
s3_client = boto3.client('s3')
bucket_name = 'pga-tournament-data'
file_keys = ['course_historical.csv', 'weather_codes.csv']

def lambda_handler(event, context):
    main()

def main():
    dataframes = {}
    
    # Read each file into a separate dataframe
    for file_key in file_keys:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')
    
        df = pd.read_csv(StringIO(file_content))
        dataframes[file_key] = df
    
    # Access the dataframes using the file keys
    course_df_0 = dataframes['course_historical.csv']
    weather_codes = dataframes['weather_codes.csv']
    
    # Initialize Open-Meteo API client
    openmeteo = openmeteo_requests.Client()
    
    # Get the current date
    today = datetime.today()
    today_minus_7 = today - timedelta(days=7)
    
    # Filter the DataFrame to include only rows with 'date_column' before today
    course_df = course_df_0[(pd.to_datetime(course_df_0['end_date']) < today) & (pd.to_datetime(course_df_0['end_date']) >= today_minus_7)]

    all_hourly_data = []
    
    for index, row in course_df.iterrows():
        # Construct the params dictionary
        params = {
            "latitude": row['latitude'],
            "longitude": row['longitude'],
            "start_date": row['start_date'],
            "end_date": row['end_date'],
            "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "rain", "weather_code", "pressure_msl", "surface_pressure", "cloud_cover", "wind_speed_10m", "wind_speed_100m", "wind_direction_10m", "wind_direction_100m", "wind_gusts_10m", "soil_temperature_0_to_7cm", "soil_moisture_7_to_28cm"]
        }
    
        url = "https://archive-api.open-meteo.com/v1/archive"
    
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
    
        # Process hourly data. The order of variables needs to be the same as requested.
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
        hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
        hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
        hourly_rain = hourly.Variables(4).ValuesAsNumpy()
        hourly_weather_code = hourly.Variables(5).ValuesAsNumpy()
        hourly_pressure_msl = hourly.Variables(6).ValuesAsNumpy()
        hourly_surface_pressure = hourly.Variables(7).ValuesAsNumpy()
        hourly_cloud_cover = hourly.Variables(8).ValuesAsNumpy()
        hourly_wind_speed_10m = hourly.Variables(9).ValuesAsNumpy()
        hourly_wind_speed_100m = hourly.Variables(10).ValuesAsNumpy()
        hourly_wind_direction_10m = hourly.Variables(11).ValuesAsNumpy()
        hourly_wind_direction_100m = hourly.Variables(12).ValuesAsNumpy()
        hourly_wind_gusts_10m = hourly.Variables(13).ValuesAsNumpy()
        hourly_soil_temperature_0_to_7cm = hourly.Variables(14).ValuesAsNumpy()
        hourly_soil_moisture_7_to_28cm = hourly.Variables(15).ValuesAsNumpy()
    
        hourly_data = {"date": pd.date_range(
          start = pd.to_datetime(hourly.Time(), unit = "s"),
          end = pd.to_datetime(hourly.TimeEnd(), unit = "s"),
          freq = pd.Timedelta(seconds = hourly.Interval()),
          inclusive = "left"
        )}
    
    
        hourly_data["latitude"] = row['latitude']
        hourly_data["longitude"] = row['longitude']
        hourly_data["year"] = row['calendar_year']
        hourly_data["event_name"] = row['event_name']
        hourly_data["tournament_id"] = row['tournament_id']
        hourly_data['start_date'] = row['start_date']
        hourly_data['end_date'] = row['end_date']
    
        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
        hourly_data["dew_point_2m"] = hourly_dew_point_2m
        hourly_data["apparent_temperature"] = hourly_apparent_temperature
        hourly_data["rain"] = hourly_rain
        hourly_data["weather_code"] = hourly_weather_code
        hourly_data["pressure_msl"] = hourly_pressure_msl
        hourly_data["surface_pressure"] = hourly_surface_pressure
        hourly_data["cloud_cover"] = hourly_cloud_cover
        hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
        hourly_data["wind_speed_100m"] = hourly_wind_speed_100m
        hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
        hourly_data["wind_direction_100m"] = hourly_wind_direction_100m
        hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m
        hourly_data["soil_temperature_0_to_7cm"] = hourly_soil_temperature_0_to_7cm
        hourly_data["soil_moisture_7_to_28cm"] = hourly_soil_moisture_7_to_28cm
    
        hourly_data["temperature_2m"] = (hourly_data["temperature_2m"] * 9/5) + 32  # convert to F
        hourly_data["apparent_temperature"] = (hourly_data["apparent_temperature"] * 9/5) + 32  # convert to F
    
        hourly_dataframe = pd.DataFrame(data = hourly_data)
    
    
        url_elevation = "https://api.open-meteo.com/v1/elevation"
        params_elevation = {
            "latitude": row['latitude'],
            "longitude": row['longitude']
        }
    
        response_elevation = requests.get(url=url_elevation, params=params_elevation)
        elevation_data = response_elevation.json()
        elevation_data = elevation_data.get('elevation')[0]
    
        hourly_data["elevation"] = elevation_data
    
        hourly_dataframe = pd.DataFrame(data = hourly_data)
    
        if hourly_dataframe.isna().any().any():
          print("DataFrame contains NaN after processing API response.")
    
        all_hourly_data.append(hourly_dataframe)
    
    # Concatenate all DataFrames into one
    final_hourly_dataframe = pd.concat(all_hourly_data, ignore_index=True)
    final_hourly_dataframe = final_hourly_dataframe.merge(weather_codes, left_on='weather_code', right_on='Code', how='inner')
    
    final_hourly_dataframe['tournament_hourly'] = final_hourly_dataframe['tournament_id'] + '_' + pd.to_datetime(final_hourly_dataframe['date']).dt.strftime('%Y%m%d%H')
    
    final_hourly_dataframe['hourly_date'] = pd.to_datetime(final_hourly_dataframe['date']).dt.strftime('%Y%m%d%H')
    
    final_hourly_dataframe_update = final_hourly_dataframe.drop(columns='date', axis=1)
    
    # Save the DataFrame to a CSV file in the /tmp directory
    final_hourly_dataframe_update.to_csv('/tmp/final_hourly_dataframe_update.csv', index=False)
    
    # Upload the file from the /tmp directory to the S3 bucket
    upload_file_to_s3('/tmp/final_hourly_dataframe_update.csv', 'pga-tournament-data', 'final_hourly_dataframe_update.csv')

    
    
def upload_file_to_s3(file_name, bucket, object_name=None):
    """
    Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified, file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except Exception as e:
        print(e)
        return False
    return True


