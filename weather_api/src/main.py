

import logging.config
import sqlite3
from utils import DATES, sql_query, insert_current_weather, COORDS, insert_last_days_weather

# Loading logging config from logging.conf file
logging.config.fileConfig('logging.conf')

# Logger
logger = logging.getLogger('root')

# Creating SQlite3 Database
connection = sqlite3.connect('weather_database.db')
cursor = connection.cursor()

# Creating Tables

# 1. Table daily_weather
try:
    q = """
    CREATE TABLE IF NOT EXISTS daily_weather (
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, 
        city_name VARCHAR(50) NOT NULL, 
        current_temp_celsius FLOAT NOT NULL, 
        min_temp_celsius FLOAT NOT NULL, 
        max_temp_celsius FLOAT NOT NULL, 
        wind_speed FLOAT
    )
    """
    cursor.execute(q)
    logging.info('Table daily_weather successfully created')
except Exception as e:
    print('Cannot create table daily_weather')
    logging.error('Error creating table daily_weather', e)

# 2. Table history_weather
try:
    q = """
    CREATE TABLE IF NOT EXISTS historic_weather (
        date VARCHAR(50) NOT NULL, 
        city_name VARCHAR(50) NOT NULL, 
        temp FLOAT NOT NULL, 
        wind_speed FLOAT
    )
    """
    cursor.execute(q)
    logging.info('Table daily_weather successfully created')
except Exception as e:
    print('Cannot create table historic_weather')
    logging.error('Error creating table historic_weather', e)

# 3. Inserting the last days weather into the database
for city in COORDS.keys():
    for date, unix_date in DATES.items():
        try:
            insert_last_days_weather(city, unix_date, date, cursor)
            connection.commit()
            logging.info(f'Success inserting values of city: {city}, date: {date}')
        except TypeError:
            print('Unexpected error occurred')
            logging.error(f'Unexpected error occurred inserting values of city: {city}, date: {date}')

# 4. Inserting the current day weather into the database
for city in ['madrid', 'barcelona']:
    try:
        insert_current_weather(city, cursor)
        logging.info(f'Success inserting values of city: {city}, date: current timestamp')
    except TypeError:
        print('Unexpected error occurred')
        logging.error('Unexpected error occurred inserting values on insert_current_weather function')

# 5. Max temperature per day
q = """
    SELECT 
        date, 
        MAX(temp), 
        city_name 
    FROM historic_weather 
    GROUP BY 1;
"""
try:
    max_temp_df = sql_query(q, cursor)
    logging.info('max_temp_df DataFrame successfully created')
except Exception as e:
    print('Error creating DataFrame')
    logging.error('Error creating max_temp_df DataFrame', e)

# 6. Average wind speed by month
q = """
    SELECT 
        AVG(wind_speed), 
        city_name, 
        strftime('%m', date) AS month 
    FROM historic_weather 
    GROUP BY 3, 2
"""
try:
    avg_wind_df = sql_query(q, cursor)
except Exception as e:
    print('Error creating DataFrame')
    logging.error('Error creating avg_wind_df DataFrame', e)

# 7. Max temperature range from the last date in the table
q = """
    SELECT 
        (max_temp_celsius - min_temp_celsius) AS temp_diff, 
        city_name, 
        created_at 
    FROM daily_weather 
    ORDER BY 1 DESC
"""
try:
    max_temp_range_df = sql_query(q, cursor)
except Exception as e:
    print('Error creating DataFrame')
    logging.error('Error creating max_temp_range_df DataFrame', e)

# Closing connection to the database
# connection.close()

try:
    print(max_temp_df)
    print(avg_wind_df)
    print(max_temp_range_df)

    sql_query('select * from daily_weather;', cursor)

except Exception as e:
    print(e)
