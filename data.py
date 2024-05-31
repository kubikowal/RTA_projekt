import csv
import pandas as pd

def extract_data(csv_file):
    data_dict = {}
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        for index, row in enumerate(reader):
            if (index + 1) % 8 == 0:
                dzien = row[0]
                city = row[1]
                data = row[2:]
                if city in data_dict:
                    data_dict[city].append(data)
                else:
                    data_dict[city] = [data]
    return data_dict, dzien

def mark_optimal_conditions(df):
    df['Optimal Conditions'] = ((df['humidity'].astype(int) > 40) & 
                                (df['avg_temp'].astype(float).between(12, 20)) & 
                                (df['wind_kph'].astype(float) < 25))
    df['Optimal Conditions'] = df['Optimal Conditions'].map({True: 'Yes', False: 'No'})
    return df

def print_combined_table(data_dict, dzien):
    combined_data = []
    for city, data in data_dict.items():
        for row in data:
            combined_data.append([city] + row)
    df = pd.DataFrame(combined_data, columns=['City', 'avg_temp', 'humidity', 'wind_kph'])
    df = mark_optimal_conditions(df)
    print(f"Data for {dzien}:")
    print(df)

if __name__ == "__main__":
    data_dict,dzien = extract_data("weather_database.csv")
    print_combined_table(data_dict,dzien)