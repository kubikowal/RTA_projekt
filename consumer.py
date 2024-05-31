import json
import csv
from kafka import KafkaConsumer
from datetime import datetime, timedelta
import subprocess
import pandas as pd

# Configuration
KAFKA_TOPIC = 'streaming'
KAFKA_BROKER = 'broker:9092'  # Update if your broker is hosted elsewhere
TRANSACTION_TOPIC = 'transaction_consumer'

def uruchom_kod(city):
    # Wpisz ścieżkę do pliku Pythona, który chcesz uruchomić
    sciezka_do_kodu = 'predictor.py'
    
    # Uruchomienie kodu i przechwycenie wyniku
    wynik = subprocess.run(['python', sciezka_do_kodu, city], capture_output=True, text=True)
    
    # Zwrócenie wyniku
    return wynik.stdout

def create_kafka_consumer(topic):
    return KafkaConsumer(topic,
                         bootstrap_servers=[KAFKA_BROKER],
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

def prepare_data(weather_data):
    # Extract features (e.g., temperature)
    data = []
    for day in weather_data:
        date = day['date']
        avg_temp = day['day']['avgtemp_c']
        data.append([date, avg_temp])
    return pd.DataFrame(data, columns=['date', 'avg_temp'])

def consume_data(topic):
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
            
            with open(csv_file, 'a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([date, city, avg_temp])

            # Execute the predictor script
            if i==7:
                result = uruchom_kod(city)
                result_json = json.loads(result)
                
                if "error" in result_json:
                    print(f"Error for {city}: {result_json['error']}")
                else:
                    predicted_temp = result_json["predicted_temp"]
                    predicted_date = result_json["predicted_date"]
                    
                    with open(csv_file, 'a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([predicted_date, city, predicted_temp])
                    
                    print(f"Predicted temperature for {city} on {predicted_date}: {predicted_temp}")
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
