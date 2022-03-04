

import pandas as pd
import requests
import time

# Unix Dates
DATES = {
    '2022-02-27 12:00:00': '1645949435',
    '2022-02-28 12:00:00': '1646035835',
    '2022-03-01 12:00:00': '1646122235',
    '2022-03-02 12:00:00': '1646208635',
    '2022-03-03 12:00:00': '1646295035'}

COORDS = {
    'madrid': ["40.416775", "-3.703790"],
    'barcelona': ["41.38879", "2.15899"]}

URL_CURRENT_DAY = "https://community-open-weather-map.p.rapidapi.com/weather"
URL_HISTORIC = "https://community-open-weather-map.p.rapidapi.com/onecall/timemachine"

HEADERS = {
    'x-rapidapi-host': "community-open-weather-map.p.rapidapi.com",
    'x-rapidapi-key': "1ca795b67amshfa6cf81e0962bf1p1e245ajsn33f3f8989a97"
    }


def insert_current_weather(city, cursor):
    try:
        querystring = {"q": "{a}, ES".format(a=city),
                       "units": "metric",
                       "mode": "json"}

        response = requests.request("GET", URL_CURRENT_DAY, headers=HEADERS, params=querystring)
        time.sleep(2)
        json_response = response.json()
        time.sleep(2)
        current_temp = json_response.get('main').get('temp')
        min_temp = json_response.get('main').get('temp_min')
        max_temp = json_response.get('main').get('temp_max')
        wind_speed = json_response.get('wind').get('speed')

        # Inserting values
        q = f""" INSERT INTO daily_weather VALUES 
            (CURRENT_TIMESTAMP, '{city}', {current_temp}, {min_temp}, {max_temp}, {wind_speed})
        """
        cursor.execute(q)

    except AttributeError:
        print('Unexpected error connecting to the API')


def insert_last_days_weather(city, unix_date, regular_date, cursor):
    try:
        if city.lower() not in COORDS.keys():
            raise NameError('City not allowed.')
        city = city.lower()
        city_lat = COORDS[city][0]
        city_lon = COORDS[city][1]

        querystring = {
            "lat": city_lat,
            "lon": city_lon,
            "dt": unix_date}
        response = requests.request("GET", URL_HISTORIC, headers=HEADERS, params=querystring)
        json_response = response.json()
        # Getting mid_day
        mid_day = json_response.get('hourly')[12]
        # Changing from UNIX Time to Regular Date
        mid_day['dt'] = regular_date
        # Inserting the data into the Database
        q = f""" INSERT INTO historic_weather VALUES 
        ('{mid_day['dt']}', '{city}', {mid_day['temp'] - 273.15}, {mid_day['wind_speed']}); 
        """
        cursor.execute(q)

    except Exception as e:
        print('Unexpected error connecting to the API', e)


def sql_query(query, cursor):
    cursor.execute(query)
    ans = cursor.fetchall()
    names = [description[0] for description in cursor.description]

    return pd.DataFrame(ans,columns=names)



