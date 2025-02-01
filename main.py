import sqlite3
import pandas as pd
from datetime import datetime, timedelta

# Ввод учебного года и начальной даты первой недели
academic_year = input("Введите учебный год (например, 2024-2025): ")
start_date_input = input("Введите дату начала первой недели (например, 20.01): ")


# Функция для определения года по месяцу
def get_year_from_month(month):
    start_year, end_year = map(int, academic_year.split('-'))
    return start_year if month in [9, 10, 11, 12] else end_year


# Преобразование введенной даты в объект datetime
start_day, start_month = map(int, start_date_input.split('.'))
start_year = get_year_from_month(start_month)
start_date = datetime(year=start_year, month=start_month, day=start_day)

# Ввод количества учебных недель
num_weeks = int(input("Введите количество учебных недель: "))

# Загружаем Excel-файл
file_path = "out.xlsx"
xls = pd.ExcelFile(file_path)
df = pd.read_excel(xls, sheet_name=xls.sheet_names[0], header=None)

# Подключение к SQLite
conn = sqlite3.connect("schedule.db")
cursor = conn.cursor()

# Создание таблицы
cursor.execute('''
CREATE TABLE IF NOT EXISTS schedule (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject TEXT,
    classroom TEXT,
    time TEXT,
    date TEXT
)
''')

# Определяем структуру данных
week_start_rows = [1, 11, 21]  # Начало каждой недели
subjects_columns = list(range(2, 14, 2))  # Четные столбцы - предметы
classroom_columns = list(range(3, 14, 2))  # Нечетные столбцы - аудитории

data_to_insert = []

# Парсинг данных по каждой неделе
for week_idx in range(num_weeks):
    current_week_start = start_date + timedelta(weeks=week_idx)
    for day_idx in range(6):  # Учебная неделя - 6 дней
        date = current_week_start + timedelta(days=day_idx)
        full_date = date.strftime('%d.%m.%Y')

        start_row = week_start_rows[week_idx]
        if start_row >= len(df):
            break

        for row in range(start_row + 2, start_row + 9):  # Проходим по строкам с парами
            if row >= len(df):
                break

            time = df.iloc[row, 1]  # Время пары
            subject = df.iloc[row, subjects_columns[day_idx]]
            classroom = df.iloc[row, classroom_columns[day_idx]]
            if pd.notna(subject) and pd.notna(time):
                data_to_insert.append((subject, classroom, time, full_date))

# Вставка данных в SQLite
cursor.executemany("INSERT INTO schedule (subject, classroom, time, date) VALUES (?, ?, ?, ?)", data_to_insert)
conn.commit()
conn.close()

print("Данные успешно сохранены в SQLite!")