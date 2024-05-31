import json
import csv
from kafka import KafkaConsumer
from datetime import datetime, timedelta
import subprocess
import pandas as pd


KAFKA_TOPIC = 'streaming'
KAFKA_BROKER = 'broker:9092'
TRANSACTION_TOPIC = 'transaction_consumer'

def uruchom_kod(city):
    sciezka_do_kodu = 'predictor.py'
    
    wynik = subprocess.run(['python', sciezka_do_kodu, city], capture_output=True, text=True)

    return wynik.stdout

def create_kafka_consumer(topic):
    return KafkaConsumer(topic,
                         bootstrap_servers=[KAFKA_BROKER],
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

def consume_data(topic):
    print("Ten program słyży do pobierania danych pogodowych oraz przewidywania na następny dzień warunków. Program pomaga rolnikom w podjęciu decyzji dotyczących planowania zabiegów ochronnych upraw w krótkim okresie czasu")
    consumer = create_kafka_consumer(topic)
    i=1
    csv_file = "weather_database.csv"
    try:
        for message in consumer:
            weather_data = message.value['data']
            city = message.value['city']
            print(f"Received weather data for {city}: {weather_data['date']}")

            date = weather_data['date']
            avg_temp = weather_data['day']['avgtemp_c']
            humidity = weather_data['day'].get('avghumidity', 0)
            wind_kph = weather_data['day'].get('maxwind_kph', 0)
            
            with open(csv_file, 'a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([date, city, avg_temp,humidity,wind_kph])

            # Execute the predictor script
            if i==7:
                result = uruchom_kod(city)
                result_json = json.loads(result)
                
                if "error" in result_json:
                    print(f"Error for {city}: {result_json['error']}")
                else:
                    predicted_temp = result_json["predicted_temp"]
                    predicted_date = result_json["predicted_date"]
                    predicted_humidity = result_json["predicted_humidity"]
                    predicted_wind = result_json["predicted_wind"]
                    
                    with open(csv_file, 'a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([predicted_date, city, predicted_temp, predicted_humidity, predicted_wind])
                    
                    print(f"Predicted data for {city} on {predicted_date}: {predicted_temp}*C,{predicted_humidity}% humidity,{predicted_wind}kph wind")
                i=0
            current_city = None
            i=i+1

            
                
    except KeyboardInterrupt:
        print("Stopped by the user.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_data(KAFKA_TOPIC)
    