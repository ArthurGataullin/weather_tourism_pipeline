import requests
import json
import os
from datetime import datetime
import unicodedata

API_KEY = "5ad8c6e531249116c886a9a28c84441b"

# Новый список городов
cities = ["New York", "London", "Munich", "Fukuoka", "Rio de Janeiro"]

def normalize_city_name(name):
    """
    Преобразует название города в безопасное имя для файла:
    - lowercase
    - заменяет пробелы на _
    - убирает диакритические знаки
    """
    name = name.lower().replace(" ", "_")
    name = ''.join(c for c in unicodedata.normalize('NFD', name)
                   if unicodedata.category(c) != 'Mn')
    return name

def collect_weather_data():
    now = datetime.now()
    timestamp_file = now.strftime("%Y%m%d_%H%M")

    base_path = "/Users/arturgataullin/PycharmProjects/PetProject/weather_tourism_pipeline/data/raw"
    path = os.path.join(base_path,
                        "openweather_api",
                        now.strftime("%Y"),
                        now.strftime("%m"),
                        now.strftime("%d"))
    os.makedirs(path, exist_ok=True)

    log_entries = []

    for city in cities:
        url = (f"http://api.openweathermap.org/data/2.5/weather?"
               f"q={city}&appid={API_KEY}&units=metric&lang=ru")
        try:
            response = requests.get(url, timeout=10)
            status = response.status_code
        except Exception as e:
            status = None
            error = str(e)

        if status == 200:
            weather_data = response.json()
            weather_data["_metadata"] = {
                "collection_time": datetime.now().isoformat(),
                "source": "openweathermap.org",
                "city_query": city
            }
            city_fname = normalize_city_name(city)
            filename = f"weather_{city_fname}_{timestamp_file}.json"
            filepath = os.path.join(path, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(weather_data, f, ensure_ascii=False, indent=2)
            log_entries.append({
                "city": city,
                "status": status,
                "file": filepath,
                "time": datetime.now().isoformat()
            })
        else:
            log_entries.append({
                "city": city,
                "status": status,
                "error": response.text if status is not None else error,
                "time": datetime.now().isoformat()
            })

    log_path = os.path.join(path, f"collect_log_{timestamp_file}.json")
    with open(log_path, "w", encoding="utf-8") as logf:
        json.dump(log_entries, logf, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    collect_weather_data()
