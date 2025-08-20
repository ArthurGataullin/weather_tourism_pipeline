#Первая реализация. Потоковая выгрузка данных с выборкой 10К строк из выборки в 100К строк
from datasets import load_dataset
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import random

# Загружаем датасет через streaming
dataset = load_dataset("irlspbru/RFSD", streaming=True)
stream = dataset["train"]

columns = [
    'age', 'line_1100', 'line_1150', 'line_1170', 'line_1190',
    'line_1200', 'line_1210', 'line_1220', 'line_1230', 'line_1300',
    'line_1400', 'line_1500', 'line_1600', 'line_1700', 'line_2100',
    'line_2110', 'line_2200', 'line_2300', 'line_2400', 'line_2500'
]

#Из выборки в 100К строк будут случайным образом отобраны 10К строк.Объём генеральной совокупности 294К строк
# Это позволит повысить репрезентативность выборки. Для оценки репрезентативности требуется стат анализ
sample_size = 10000
max_stream_rows = 100000  # читаем только первые 100k строк для ускорения
reservoir = []

for i, row in enumerate(stream):
    if i >= max_stream_rows:
        break

    entry = {col: row.get(col, None) for col in columns}

    if i < sample_size:
        reservoir.append(entry)
    else:
        j = random.randint(0, i)
        if j < sample_size:
            reservoir[j] = entry

# Формируем DataFrame
df = pd.DataFrame(reservoir)
print("Размер выборки до фильтрации:", df.shape)
print(df.head())

# # Фильтрация: исключаем строки, где line_2110 == NaN или 0 (выручка компании)
# df = df[df["line_2110"].notna() & (df["line_2110"] != 0)]
#
# print("Размер выборки после фильтрации:", len(df))

# 1. Описательная статистика
print("Описательная статистика:")
desc = df.describe(include='all').transpose()
print(desc)
desc.to_csv("descriptive_statistics_1.csv", index=True)

# 2. Гистограммы для трех колонок (берем первые три числовые)
# numeric_cols = df.select_dtypes(include='number').columns[:3]
numeric_cols = ['age', 'line_2110', 'line_2400']
for col in numeric_cols:
    plt.figure(figsize=(6, 4))
    sns.histplot(df[col], kde=True, bins=30)
    plt.title(f'Гистограмма колонки {col}')
    plt.show()

    # Проверка на выбросы
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    outliers = df[(df[col] < Q1 - 1.5 * IQR) | (df[col] > Q3 + 1.5 * IQR)]
    print(f"Колонка {col}, выбросы: {len(outliers)}")

# 3. Корреляционная матрица
corr_matrix = df.corr()
plt.figure(figsize=(12, 10))
sns.heatmap(corr_matrix, annot=True, fmt=".2f", cmap="coolwarm")
plt.title("Матрица корреляций")
plt.show()

# # 4. Scatter plot для сильно коррелирующих колонок (например, корр > 0.7)
# high_corr = [(i, j) for i in corr_matrix.columns for j in corr_matrix.columns if
#              i != j and abs(corr_matrix.loc[i, j]) > 0.7]
# for col1, col2 in high_corr[:3]:  # строим первые 3 пары
#     plt.figure(figsize=(6, 4))
#     sns.scatterplot(data=df, x=col1, y=col2)
#     plt.title(f'Scatter plot {col1} vs {col2}')
#     plt.show()

# 5. Вычисляем коэффициент корреляции для выручки и прибыли
plt.figure(figsize=(6, 4))
sns.scatterplot(data=df, x='line_2110', y='line_2400')
plt.title('Scatter plot line_2110 vs line_2400')
plt.xlabel('line_2110')
plt.ylabel('line_2400')
plt.show()
