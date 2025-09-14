import pandas as pd
import os
from datetime import datetime

enriched_base = "/Users/arturgataullin/PycharmProjects/PetProject/weather_tourism_pipeline/data/enriched"
aggregated_base = "/Users/arturgataullin/PycharmProjects/PetProject/weather_tourism_pipeline/data/aggregated"

def create_aggregated_views():
    now = datetime.now()
    enriched_file = f"weather_enriched_{now.strftime('%Y%m%d')}.csv"
    enriched_path = os.path.join(enriched_base, enriched_file)

    if not os.path.exists(enriched_path):
        print(f"Файл {enriched_path} не найден")
        return

    df = pd.read_csv(enriched_path)

    os.makedirs(aggregated_base, exist_ok=True)

    # ================= Витрина 1: Рейтинг городов =================
    rating_df = df[[
        "city_name", "comfort_index", "recommended_activity", "tourism_season"
    ]].copy()
    rating_df = rating_df.sort_values(by="comfort_index", ascending=False)
    rating_path = os.path.join(aggregated_base, "city_tourism_rating.csv")
    rating_df.to_csv(rating_path, index=False, encoding="utf-8")

    # ================= Витрина 2: Сводка по федеральным округам =========
    summary = df.groupby("federal_district").agg(
        avg_temperature=("temperature", "mean"),
        comfortable_cities=("comfort_index", lambda x: (x.fillna(0) > 70).sum()),
        total_cities=("city_name", "count")
    ).reset_index()

    def general_recommendation(row):
        if row["avg_temperature"] > 15:
            return "Отлично для туров"
        elif row["avg_temperature"] > 5:
            return "Умеренно комфортно"
        else:
            return "Лучше музеи/индор"

    summary["general_recommendation"] = summary.apply(general_recommendation, axis=1)
    summary_path = os.path.join(aggregated_base, "federal_districts_summary.csv")
    summary.to_csv(summary_path, index=False, encoding="utf-8")

    # ================= Витрина 3: Отчет для турагентств =================
    top3 = df.sort_values(by="comfort_index", ascending=False).head(3)[
        ["city_name", "comfort_index", "recommended_activity"]
    ]

    stay_home = df[df["comfort_index"] < 40][
        ["city_name", "comfort_index", "recommended_activity"]
    ]

    # Специальные рекомендации
    def special_recommendation(row):
        wind = row.get("wind_speed", 0)
        desc = str(row.get("weather_description", "")).lower()
        temp = row.get("temperature", 0)

        if pd.notna(wind) and wind > 8:
            return "Ветрено — взять ветровку"
        elif "дожд" in desc:
            return "Возможен дождь — взять зонт"
        elif pd.notna(temp) and temp < 5:
            return "Холодно — теплая одежда"
        else:
            return "Погода благоприятна"

    df["special_recommendation"] = df.apply(special_recommendation, axis=1)

    top3_df = pd.DataFrame({
        "type": ["Топ-3 города"] * len(top3),
        "city": top3["city_name"].values,
        "comfort_index": top3["comfort_index"].values,
        "recommendation": top3["recommended_activity"].values
    })

    stay_home_df = pd.DataFrame({
        "type": ["Лучше остаться дома"] * len(stay_home),
        "city": stay_home["city_name"].values,
        "comfort_index": stay_home["comfort_index"].values,
        "recommendation": stay_home["recommended_activity"].values
    })

    recommendations_df = pd.concat([top3_df, stay_home_df], ignore_index=True)

    recommendations_path = os.path.join(aggregated_base, "travel_recommendations.csv")
    recommendations_df.to_csv(recommendations_path, index=False, encoding="utf-8")

    print("Витрины созданы:")
    print(" -", rating_path)
    print(" -", summary_path)
    print(" -", recommendations_path)


if __name__ == "__main__":
    create_aggregated_views()
