import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from datetime import timedelta
import json

df = pd.read_csv('weather_database.csv', parse_dates=[0], skiprows=1, header=None, names=['date', 'city', 'avg_temp','humidity','wind_kph'])
df.set_index('date', inplace=True)

def train_model(data,feature):
    X = np.arange(len(data)).reshape(-1, 1)
    y = data[feature].values
    model = LinearRegression()
    model.fit(X, y)
    return model

def predict_weather(model, last_date_index):
    next_day_index = np.array([[last_date_index + 1]])
    predicted_value = model.predict(next_day_index)
    return predicted_value[0]

def main(city):
    try:
        city_data = df[df['city'] == city]
        
        if len(city_data) < 5:
            print(f"Nie wystarczająca ilość danych dla {city} do trenowania modelu.")
            return
        
        temp_model = train_model(city_data, 'avg_temp')
        humidity_model = train_model(city_data, 'humidity')
        wind_model = train_model(city_data, 'wind_kph')
        
        last_date = city_data.index[-1]
        predicted_temp = round(predict_weather(temp_model, len(city_data) - 1), 1)
        predicted_humidity = round(predict_weather(humidity_model, len(city_data) - 1),1)
        predicted_wind = round(predict_weather(wind_model, len(city_data) - 1), 1)

        prediction_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        
        result = {"predicted_temp": predicted_temp, "predicted_date": prediction_date, "predicted_humidity": predicted_humidity, "predicted_wind": predicted_wind}
        print(json.dumps(result))
    
    except Exception as e:
        print(f"Wystąpił błąd: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Podaj nazwę miasta jako argument.")
        sys.exit(1)
    city = sys.argv[1]
    main(city)