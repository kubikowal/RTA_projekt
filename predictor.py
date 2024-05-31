import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from datetime import timedelta
import json

df = pd.read_csv('weather_database.csv', parse_dates=[0], skiprows=1, header=None, names=['date', 'city', 'avg_temp'])
df.set_index('date', inplace=True)

def train_model(data):
    # Train a simple linear regression model
    X = np.arange(len(data)).reshape(-1, 1)
    y = data['avg_temp'].values
    model = LinearRegression()
    model.fit(X, y)
    return model

def predict_weather(model, last_date_index):
    # Predict the next day's weather
    next_day_index = np.array([[last_date_index + 1]])
    predicted_temp = model.predict(next_day_index)
    return predicted_temp[0]

def main(city):
    try:
        # Filtruj dane dla danego miasta
        city_data = df[df['city'] == city]
        
        if len(city_data) < 5:
            print(f"Nie wystarczająca ilość danych dla {city} do trenowania modelu.")
            return
        
        # Trenuj model
        model = train_model(city_data)
        
        # Predykcja temperatury na następny dzień
        last_date = city_data.index[-1]
        predicted_temp = round(predict_weather(model, len(city_data) - 1), 1)
        
        # Data predykcji
        prediction_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Wydrukowanie wyników predykcji
        result = {"predicted_temp": predicted_temp, "predicted_date": prediction_date}
        print(json.dumps(result))
    
    except Exception as e:
        print(f"Wystąpił błąd: {e}")

if __name__ == "__main__":
    # Musimy otrzymać nazwę miasta jako argument zewnętrzny
    import sys
    if len(sys.argv) < 2:
        print("Podaj nazwę miasta jako argument.")
        sys.exit(1)
    city = sys.argv[1]
    main(city)