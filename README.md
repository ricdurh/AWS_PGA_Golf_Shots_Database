# AWS_PGA_Shots_Database

The objective of this project is to create a data pipeline to host 3 tables on AWS Redshift:

        1) PGA Tour shot level data for 20 players from 2020-2024 (currently > 340,000 shots)
        2) PGA Tour tee time data
        3) hourly weather data for each tournament


This process can be broken down to the following steps: 

        1. Upload initial files to S3 Buckets,
        2. Use 3 different Python scripts to scrape data and be used via AWS Lambda functions
        3. Create AWS Redshift tables
        4. Create weekly schedules in CloudWatch to trigger Lambda functions
        5. Schedule weekly Glue jobs to update Redshift tables

This project was motivated from a blog post by Alex Lohec who was looking to scrape the PGA TOUR's TOURCast web application. Similar to what he [shared](https://alexlohec.com/posts/2021-04-14-scrape/), this project is not for commercial use and I'm not making any of this data public. For those reasons, I feel comfortable accessing the public data on the internet, but respect the PGA's decision to keep control over their data. 

## Data
The weather data comes from the [Open-Meteo](https://open-meteo.com/) Historical Weather and Elevation APIs. All code used to get this hourly weather data is shared in the "lambda_functions" folder. The tee time and shot-level scripts are not shared to respect the privacy and servers of the PGA Tour. The first five rows of the three tables can be found in the "data" folder. \

## Process
The first step in the process was to grab the golf course schedule data from datagolf.com's [API](https://datagolf.com/api-access). Using the dates of the tournament, the hourly weather data is scraped for each hour of the tournament. Lambda functions were used to upload the initial data to an S3 bucket for the weather and tee time data. The shot level data was run locally due to the size of the data, and to respect the servers as much as possible. With the initial data in S3, the three tables were created in Redshift. Lambda functions were created to scrape new data for the previous week. These are triggered from a weekly CloudWatch schedule Monday morning. Finally, weekly Glue jobs were created to update the Redshift tables automatically on a weekly basis. 
