import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import pytz
from ics import Calendar, Event

def get_year_from_month(month, academic_year):
    start_year, end_year = map(int, academic_year.split('-'))
    return start_year if month in [9, 10, 11, 12] else end_year

def load_excel_file(file_path):
    xls = pd.ExcelFile(file_path)
    df = pd.read_excel(xls, sheet_name=xls.sheet_names[0], header=None)
    return df

def save_schedule_to_db(conn, data_to_insert):
    cursor = conn.cursor()
    cursor.executemany("INSERT INTO schedule (subject, classroom, time, date, teacher) VALUES (?, ?, ?, ?, ?)", data_to_insert)
    conn.commit()

def process_schedule(df, start_date, num_weeks):
    week_start_rows = [1, 11, 21]  # Начало каждой учебной недели
    subjects_columns = list(range(2, 14, 2))
    classroom_columns = list(range(3, 14, 2))
    data_to_insert = []

    for week_idx in range(num_weeks):
        current_week_start = start_date + timedelta(weeks=week_idx)
        start_row = week_start_rows[week_idx]
        if start_row >= len(df): break
        for day_idx in range(6):
            date = current_week_start + timedelta(days=day_idx)
            full_date = date.strftime('%d.%m.%Y')
            for row in range(start_row + 1, start_row + 9):
                if row >= len(df): break
                time = df.iloc[row, 1]
                subject = df.iloc[row, subjects_columns[day_idx]]
                classroom = df.iloc[row, classroom_columns[day_idx]]
                if pd.notna(subject) and pd.notna(time):
                    data_to_insert.append((subject, classroom, time, full_date, None))
    return data_to_insert

def find_all_headers(df, header="Преподаватель"):
    positions = []
    for col in range(df.shape[1]):
        for i, value in enumerate(df.iloc[:, col].astype(str).str.strip()):
            if value == header:
                positions.append((i, col))
    return positions

def process_teachers(df):
    header_positions = find_all_headers(df)
    if len(header_positions) < 2:
        raise ValueError("Недостаточно заголовков 'Преподаватель'.")
    (start_row_table1, col_table1), (start_row_table2, col_table2) = header_positions[:2]
    df_table1 = df.iloc[start_row_table1 + 1:, col_table1:col_table1 + 2].dropna(how='all').reset_index(drop=True)
    df_table2 = df.iloc[start_row_table2 + 1:, col_table2:col_table2 + 2].dropna(how='all').reset_index(drop=True)
    df_table1.columns = ["Преподаватель", "Дисциплина"]
    df_table2.columns = ["Преподаватель", "Дисциплина"]
    df_combined = pd.concat([df_table1, df_table2]).drop_duplicates().reset_index(drop=True)
    return df_combined

def map_teachers(schedule_data, teacher_data):
    teacher_map_by_prefix = {}

    for _, row in teacher_data.iterrows():
        discipline = row["Дисциплина"]
        teacher = row["Преподаватель"]
        key = discipline[:3]
        if key not in teacher_map_by_prefix:
            teacher_map_by_prefix[key] = teacher

    exception_map = {
        "МПК_англ": "Межкульт_проф_комм",
        "МПК_англ_зачёт": "Межкульт_проф_комм",
        "Р_и_АТ_к_ПО_экз": "Разр_и_ан_треб_к_ПО",
        "ОПД_зачёт_ОНЛАЙН": "Осн_проект_деятель",
        "БД_1 подгруппа": "Базы данных",
        "БД_2 подгруппа": "Базы данных"
    }

    exception_teacher_map = {}
    for exception_key, exception_value in exception_map.items():
        filtered = teacher_data[teacher_data['Дисциплина'] == exception_value]
        if not filtered.empty:
            exception_teacher_map[exception_key] = filtered['Преподаватель'].values[0]

    for idx, entry in enumerate(schedule_data):
        subject = entry[0]
        key = subject[:3]
        if key in teacher_map_by_prefix:
            teacher = teacher_map_by_prefix[key]
        elif subject in exception_teacher_map:
            teacher = exception_teacher_map[subject]
        else:
            teacher = "NA"
        schedule_data[idx] = entry[:-1] + (teacher,)

def convert_time_format(time_str):
    return time_str.replace('.', ':')

def create_ical_file(rows, output_path, timezone_offset):
    calendar = Calendar()
    tz = pytz.timezone(f'Etc/GMT{-timezone_offset}')

    for row in rows:
        subject, classroom, time, date_str, teacher = row
        event_name = f"{subject} ({teacher})"
        location = classroom
        start_time, end_time = map(convert_time_format, map(str.strip, time.split('-')))
        start_datetime = datetime.strptime(f"{date_str} {start_time}", "%d.%m.%Y %H:%M")
        end_datetime = datetime.strptime(f"{date_str} {end_time}", "%d.%m.%Y %H:%M")
        start_datetime = tz.localize(start_datetime)
        end_datetime = tz.localize(end_datetime)

        event = Event()
        event.name = event_name
        event.location = location
        event.begin = start_datetime
        event.end = end_datetime
        calendar.events.add(event)

    with open(output_path, 'w') as ics_file:
        ics_file.writelines(calendar)
    print(f"iCalendar файл успешно создан: {output_path}")

def main():
    academic_year = input("Введите учебный год (например, 2024-2025): ")
    start_date_input = input("Введите дату начала первой недели (например, 20.01): ")
    start_day, start_month = map(int, start_date_input.split('.'))
    start_year = get_year_from_month(start_month, academic_year)
    start_date = datetime(year=start_year, month=start_month, day=start_day)
    num_weeks = int(input("Введите количество учебных недель: "))
    file_path = "out.xlsx"
    df = load_excel_file(file_path)

    # Подключение к SQLite
    conn = sqlite3.connect("schedule.db")
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS schedule (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        subject TEXT,
                        classroom TEXT,
                        time TEXT,
                        date TEXT,
                        teacher TEXT)''')

    schedule_data = process_schedule(df, start_date, num_weeks)
    teacher_data = process_teachers(df)
    map_teachers(schedule_data, teacher_data)

    save_schedule_to_db(conn, schedule_data)
    teacher_data.to_sql("teachers", conn, if_exists="replace", index=False)

    cursor.execute("SELECT subject, classroom, time, date, teacher FROM schedule")
    rows = cursor.fetchall()
    conn.close()

    output_path = "schedule.ics"
    timezone_offset = 4  # Часовой пояс +4
    create_ical_file(rows, output_path, timezone_offset)

if __name__ == '__main__':
    main()