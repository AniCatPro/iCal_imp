import pandas as pd
import sqlite3

# Путь к файлу
file_path = "out.xlsx"
sheet_name = "Б22-191-1з, 2з"

df = pd.read_excel(file_path, sheet_name=sheet_name)

# Функция поиска всех вхождений заголовка
def find_all_headers(df, header="Преподаватель"):
    positions = []
    for col in range(df.shape[1]):  # Перебираем все колонки
        for i, value in enumerate(df.iloc[:, col].astype(str).str.strip()):
            if value == header:
                positions.append((i, col))  # Добавляем найденную позицию
    return positions

# Ищем все заголовки "Преподаватель"
header_positions = find_all_headers(df)

if len(header_positions) < 2:
    raise ValueError("Найдено недостаточно заголовков 'Преподаватель'. Возможно, таблицы не совпадают с ожиданиями.")

(start_row_table1, col_table1), (start_row_table2, col_table2) = header_positions[:2]

print(f"Таблица 1 найдена: строка {start_row_table1}, колонка {col_table1}")
print(f"Таблица 2 найдена: строка {start_row_table2}, колонка {col_table2}")

# Загружаем таблицу 1
columns_table1 = ["Преподаватель", "Дисциплина", "Аттест"]
df_table1 = df.iloc[start_row_table1+1:, col_table1:col_table1+3].dropna(how='all').reset_index(drop=True)
df_table1.columns = columns_table1

# Загружаем таблицу 2
columns_table2 = ["Преподаватель", "Дисциплина", "Аттест", "Лек", "Лабы"]
df_table2 = df.iloc[start_row_table2+1:, col_table2:col_table2+5].dropna(how='all').reset_index(drop=True)
df_table2.columns = columns_table2

# Подключение к SQLite
conn = sqlite3.connect("schedule_tb.db")

# Экспорт данных
df_table1.to_sql("table1", conn, if_exists="replace", index=False)
df_table2.to_sql("table2", conn, if_exists="replace", index=False)

# Закрываем соединение
conn.close()

print("Данные успешно экспортированы в SQLite")
