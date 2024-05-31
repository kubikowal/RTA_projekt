import json
import time
from datetime import datetime, timedelta
import requests
from kafka import KafkaProducer

API_KEY = 'b3ac3cda83384b7a976181031242105'
KAFKA_TOPIC = 'streaming'
KAFKA_BROKER = 'broker:9092'
CAPITALS = [
    "Kabul", "Tirana", "Algiers", "Andorra la Vella", "Luanda", "Saint John's", "Buenos Aires", "Yerevan",
    "Canberra", "Vienna", "Baku", "Nassau", "Manama", "Dhaka", "Bridgetown", "Minsk", "Brussels", "Belmopan",
    "Porto-Novo", "Thimphu", "Sucre", "Sarajevo", "Gaborone", "Brasília", "Bandar Seri Begawan", "Sofia",
    "Ouagadougou", "Gitega", "Praia", "Phnom Penh", "Yaoundé", "Ottawa", "Bangui", "N'Djamena", "Santiago",
    "Beijing", "Bogotá", "Moroni", "Kinshasa", "Brazzaville", "San José", "Zagreb", "Havana", "Nicosia", 
    "Prague", "Copenhagen", "Djibouti", "Roseau", "Santo Domingo", "Quito", "Cairo", "San Salvador", 
    "Malabo", "Asmara", "Tallinn", "Mbabane", "Addis Ababa", "Palikir", "Suva", "Helsinki", "Paris", 
    "Libreville", "Banjul", "Tbilisi", "Berlin", "Accra", "Athens", "Saint George's", "Guatemala City", 
    "Conakry", "Bissau", "Georgetown", "Port-au-Prince", "Tegucigalpa", "Budapest", "Reykjavik", "New Delhi", 
    "Jakarta", "Tehran", "Baghdad", "Dublin", "Jerusalem", "Rome", "Kingston", "Tokyo", "Amman", "Nur-Sultan", 
    "Nairobi", "Tarawa", "Pyongyang", "Seoul", "Pristina", "Kuwait City", "Bishkek", "Vientiane", "Riga", 
    "Beirut", "Maseru", "Monrovia", "Tripoli", "Vaduz", "Vilnius", "Luxembourg", "Antananarivo", "Lilongwe", 
    "Kuala Lumpur", "Malé", "Bamako", "Valletta", "Majuro", "Nouakchott", "Port Louis", "Mexico City", 
    "Palikir", "Chisinau", "Monaco", "Ulaanbaatar", "Podgorica", "Rabat", "Maputo", "Naypyidaw", "Windhoek", 
    "Yaren", "Kathmandu", "Amsterdam", "Wellington", "Managua", "Niamey", "Abuja", "Skopje", "Oslo", "Muscat", 
    "Islamabad", "Ngerulmud", "Jerusalem", "Panama City", "Port Moresby", "Asunción", "Lima", "Manila", 
    "Warsaw", "Lisbon", "Doha", "Bucharest", "Moscow", "Kigali", "Basseterre", "Castries", "Kingstown", 
    "Apia", "San Marino", "São Tomé", "Riyadh", "Dakar", "Belgrade", "Victoria", "Freetown", "Singapore", 
    "Bratislava", "Ljubljana", "Honiara", "Mogadishu", "Pretoria", "Juba", "Madrid", "Colombo", "Khartoum", 
    "Paramaribo", "Stockholm", "Bern", "Damascus", "Taipei", "Dushanbe", "Dodoma", "Bangkok", "Lomé", 
    "Nukuʻalofa", "Port of Spain", "Tunis", "Ankara", "Ashgabat", "Funafuti", "Kampala", "Kyiv", "Abu Dhabi", 
    "London", "Washington, D.C.", "Montevideo", "Tashkent", "Port Vila", "Vatican City", "Caracas", "Hanoi", 
    "Sanaa", "Lusaka", "Harare"
]
weather_data = []
def fetch_weather_data(city, date):
    url = f"http://api.weatherapi.com/v1/history.json?key={API_KEY}&q={city}&dt={date}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data for {city} on {date}: {response.text}")
        return None

def create_kafka_producer():
    return KafkaProducer(bootstrap_servers=[KAFKA_BROKER],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def send_weather_data(producer, city, weather_data):
    message = {'city': city, 'data': weather_data}
    producer.send(KAFKA_TOPIC, value=message)
    producer.flush()
    print(f"Sent data for {city}: {weather_data['date']}")

def main():
    producer = create_kafka_producer()
    try:
        while True:
            current_date = datetime.now()
            for city in CAPITALS:
                for days_ago in range(7, 0, -1):
                    date_to_fetch = (current_date - timedelta(days=days_ago)).strftime('%Y-%m-%d')
                    weather_data = fetch_weather_data(city, date_to_fetch)
                    if weather_data:
                        send_weather_data(producer, city, weather_data['forecast']['forecastday'][0])
                        time.sleep(1)
            return weather_data
    except KeyboardInterrupt:
        print("Stopped by the user.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()