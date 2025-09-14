import pandas as pd
import os
from datetime import datetime

# Пути
cleaned_base = "/Users/arturgataullin/PycharmProjects/PetProject/weather_tourism_pipeline/data/cleaned"
enriched_base = "/Users/arturgataullin/PycharmProjects/PetProject/weather_tourism_pipeline/data/enriched"
reference_file = os.path.join(enriched_base, "city_reference.csv")

def calculate_comfort_index(temp, humidity, wind):
    """Простая формула комфортности"""
    return 100 - abs(temp - 22)*2 - abs(humidity - 50)*0.2 - wind*3

def recommended_activity(ci):
    """Рекомендации по типу активности"""
    if ci > 70:
        return "Прогулки"
    elif ci > 40:
        return "Музеи/индор"
    else:
        return "Домашний отдых"

def check_tourism_season(season_str, date):
    """Проверка, подходит ли текущий месяц для туризма"""
    if not isinstance(season_str, str) or not season_str.strip():
        return "Нет"

    season_str = season_str.strip()
    if season_str == "Круглогодично":
        return "Да"

    months_map = {
        "Январь":1,"Февраль":2,"Март":3,"Апрель":4,"Май":5,"Июнь":6,
        "Июль":7,"Август":8,"Сентябрь":9,"Октябрь":10,"Ноябрь":11,"Декабрь":12
    }

    parts = season_str.split("-")
    if len(parts) == 2:
        start_m = months_map.get(parts[0], 0)
        end_m = months_map.get(parts[1], 0)
        m = date.month
        if start_m == 0 or end_m == 0:
            return "Нет"
        if start_m <= end_m:
            return "Да" if start_m <= m <= end_m else "Нет"
        else:  # сезон через Новый год
            return "Да" if m >= start_m or m <= end_m else "Нет"

    return "Нет"

def enrich_weather_data():
    now = datetime.now()
    cleaned_file = f"weather_cleaned_{now.strftime('%Y%m%d')}.csv"
    cleaned_path = os.path.join(cleaned_base, cleaned_file)

    if not os.path.exists(cleaned_path):
        print(f"Файл {cleaned_path} не найден")
        return

    df = pd.read_csv(cleaned_path)

    # Загружаем справочник городов
    if not os.path.exists(reference_file):
        print(f"Справочник {reference_file} не найден")
        return

    ref = pd.read_csv(reference_file)

    # Проверяем соответствие city_name
    missing_cities = set(df['city_name']) - set(ref['city_name'])
    if missing_cities:
        print(f"В справочнике отсутствуют города: {missing_cities}")

    # Объединяем данные по city_name
    df = df.merge(ref, on="city_name", how="left")

    # Добавляем вычисляемые поля
    df["comfort_index"] = df.apply(
        lambda row: round(calculate_comfort_index(
            row["temperature"], row["humidity"], row["wind_speed"]
        )), axis=1)

    df["recommended_activity"] = df["comfort_index"].apply(recommended_activity)
    df["tourist_season_match"] = df.apply(
        lambda row: check_tourism_season(str(row.get("tourism_season", "") or ""), now),
        axis=1
    )

    # Создаем папку enriched
    os.makedirs(enriched_base, exist_ok=True)
    enriched_file = f"weather_enriched_{now.strftime('%Y%m%d')}.csv"
    enriched_path = os.path.join(enriched_base, enriched_file)
    df.to_csv(enriched_path, index=False, encoding="utf-8")

    print(f"Enriched CSV сохранен: {enriched_path}")
    print(f"Обработано городов: {len(df)}")

if __name__ == "__main__":
    enrich_weather_data()
