import csv
import requests
from datetime import datetime



def api_data_to_csv():
    api_url = "https://community-open-weather-map.p.rapidapi.com/weather"
    api_key = "bead41229dmshc0b659a5652458ep14f4b9jsn4b3015e6d74d"
    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': "community-open-weather-map.p.rapidapi.com",
    }

    places = ["Udhampur","Jammu","Mumbai","Bangalore","Delhi","Srinagar","Punjab","Goa","Patna","Kolkata"]
    header = ['City-Name','Description','Temperature','Feels Like Temperature','Min Temperature','Max Temperature', 'Humidity', 'Clouds']
    data_In_JSON=[]

    for place in places:
        try:
            query = {"q":f"{place},india","lat":"0","lon":"0","id":"2172797","lang":"null","units":"imperial","mode":"JSON"}
            response = requests.get(api_url, headers=headers, params=query)
            data = response.json()
            item=[data['name'],data['weather'][0]['description'],data['main']['temp'],data['main']['feels_like'],data['main']['temp_min'],
                  data['main']['temp_max'],data['main']['humidity'],data['clouds']['all']]
            data_In_JSON.append(item)
            print(item)
        except:
            print("Exceeded the rate limit per minute provided by API provider ")

    try:
        with open('/usr/local/airflow/store_files_airflow/weather_data.csv', 'w', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(data_In_JSON)


    except:
       print("some error occured")

if __name__=="__main__":
    api_data_to_csv()