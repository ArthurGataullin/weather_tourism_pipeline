import os
import json
import pandas as pd
from datetime import datetime
import unicodedata

# Папка RAW
raw_base = "/Users/arturgataullin/PycharmProjects/PetProject/weather_tourism_pipeline/data/raw/openweather_api"
# Папка CLEANED
cleaned_base = "/Users/arturgataullin/PycharmProjects/PetProject/weather_tourism_pipeline/data/cleaned"

# Словарь стандартизации названий городов (можно оставить английские имена или добавить переводы)
city_map = {
    "new_york": "Нью-Йорк",
    "london": "Лондон",
    "munich": "Мюнхен",
    "fukuoka": "Фукуока",
    "rio_de_janeiro": "Рио-де-Жанейро"
}

def normalize_city_key(name):
    """Приведение названия города к ключу словаря"""
    name = name.lower().replace(" ", "_")
    # Убираем диакритические знаки
    name = ''.join(c for c in unicodedata.normalize('NFD', name) if unicodedata.category(c) != 'Mn')
    return name

def mm_hg_from_hpa(hpa):
    """Перевод гПа -> мм рт. ст."""
    return round(hpa * 0.75006)

def clean_weather_data():
    now = datetime.now()
    today_folder = os.path.join(raw_base,
                                now.strftime("%Y"),
                                now.strftime("%m"),
                                now.strftime("%d"))
    files = [f for f in os.listdir(today_folder) if f.endswith(".json") and f.startswith("weather_")]

    records = []
    problems = []

    for file in files:
        filepath = os.path.join(today_folder, file)
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        city_query = data.get("_metadata", {}).get("city_query", "")
        city_key = normalize_city_key(city_query)
        city_name = city_map.get(city_key, city_query)  # если нет перевода, оставляем как есть

        main = data.get("main", {})
        wind = data.get("wind", {})
        weather_list = data.get("weather", [])

        temp = main.get("temp")
        feels_like = main.get("feels_like")
        humidity = main.get("humidity")
        pressure = main.get("pressure")
        wind_speed = wind.get("speed")
        description = weather_list[0].get("description") if weather_list else None
        collection_time = data.get("_metadata", {}).get("collection_time")

        try:
            temp = int(round(temp))
            feels_like = int(round(feels_like))
        except:
            problems.append(f"Проблема с температурой в {file}")
            continue

        if temp < -50 or temp > 60:
            problems.append(f"Температура вне диапазона в {file}: {temp}")
            continue

        if pressure is not None:
            pressure = mm_hg_from_hpa(pressure)

        try:
            collection_time = datetime.fromisoformat(collection_time).strftime("%Y-%m-%d %H:%M:%S")
        except:
            problems.append(f"Неверный формат времени {file}")
            collection_time = None

        records.append({
            "city_name": city_name,
            "temperature": temp,
            "feels_like": feels_like,
            "humidity": humidity,
            "pressure": pressure,
            "wind_speed": wind_speed,
            "weather_description": description,
            "collection_time": collection_time
        })

    os.makedirs(cleaned_base, exist_ok=True)

    csv_name = f"weather_cleaned_{now.strftime('%Y%m%d')}.csv"
    csv_path = os.path.join(cleaned_base, csv_name)
    df = pd.DataFrame(records)
    df.to_csv(csv_path, index=False, encoding="utf-8")

    log_name = f"cleaning_log_{now.strftime('%Y%m%d')}.txt"
    log_path = os.path.join(cleaned_base, log_name)
    with open(log_path, "w", encoding="utf-8") as logf:
        logf.write(f"Количество исходных записей: {len(files)}\n")
        logf.write(f"Количество очищенных записей: {len(records)}\n")
        logf.write("Примененные правила: температура целое число, стандартизация названий, "
                   "единый формат времени, проверка диапазона температуры (-50…+60°C), "
                   "давление в мм рт. ст.\n")
        if problems:
            logf.write("Найденные проблемы:\n")
            for p in problems:
                logf.write(f"- {p}\n")
        else:
            logf.write("Найденные проблемы: не обнаружено\n")

    print(f"CSV сохранен: {csv_path}")
    print(f"Лог сохранен: {log_path}")

if __name__ == "__main__":
    clean_weather_data()
