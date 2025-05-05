import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import pytz
from ics import Calendar, Event
from fuzzywuzzy import process


# Функция для получения года на основании месяца и учебного года
def get_year_from_month(month, academic_year):
    start_year, end_year = map(int, academic_year.split('-'))
    return start_year if month in [9, 10, 11, 12] else end_year


# Загрузка Excel файла
def load_excel_file(file_path):
    xls = pd.ExcelFile(file_path)
    df = pd.read_excel(xls, sheet_name=xls.sheet_names[0], header=None)
    return df


# Сохранение расписания в базу данных
def save_schedule_to_db(conn, data_to_insert):
    cursor = conn.cursor()
    cursor.executemany(
        "INSERT INTO schedule (subject, classroom, time, date, teacher, type, presence, subgroups) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        data_to_insert
    )
    conn.commit()


# Извлечение атрибутов из названия предмета
def extract_attributes(subject):
    # Определение суффиксов и соответствующих значений
    types_map = {
        "зач_КР": "Сдача курсовой работы",
        "_зач с оц": "Зачет с оценкой",
        "_экз": "Экзамен",
        "_зачёт": "Зачет"
    }
    presence_map = {"_ОНЛАЙН": "Онлайн"}
    subgroups_map = {
        "_1 подгруп": "Подгруппа 1",
        "1 подгруп": "Подгруппа 1",
        "_2 подгруп": "Подгруппа 2",
        "2 подгруп": "Подгруппа 2"
    }

    type_value = "Лекция"
    presence_value = "Очно"
    subgroups_value = ""

    # Удаление суффиксов по картам
    for type_key in types_map:
        if type_key in subject:
            type_value = types_map[type_key]
            subject = subject.replace(type_key, "")

    for presence_key in presence_map:
        if presence_key in subject:
            presence_value = presence_map[presence_key]
            subject = subject.replace(presence_key, "")

    for subgroups_key in subgroups_map:
        if subgroups_key in subject:
            subgroups_value = subgroups_map[subgroups_key]
            subject = subject.replace(subgroups_key, "")

    # Удаляем пробелы в начале и конце, а также любые лишние пробелы
    subject = subject.strip().replace(" ", " ")
    return subject, type_value, presence_value, subgroups_value


# Обработка расписания
def process_schedule(df, start_date, week_indices):
    week_start_rows = [1, 11, 21]  # Начало каждой учебной недели
    subjects_columns = list(range(2, 14, 2))
    classroom_columns = list(range(3, 14, 2))
    data_to_insert = []

    for week_idx in week_indices:
        if week_idx - 1 >= len(week_start_rows):
            break

        current_week_start = start_date + timedelta(weeks=week_idx - 1)
        start_row = week_start_rows[week_idx - 1]

        for day_idx in range(6):
            date = current_week_start + timedelta(days=day_idx)
            full_date = date.strftime('%d.%m.%Y')

            for row in range(start_row + 1, start_row + 9):
                if row >= len(df):
                    break

                time = df.iloc[row, 1]
                raw_subject = df.iloc[row, subjects_columns[day_idx]]
                classroom = df.iloc[row, classroom_columns[day_idx]]

                if pd.notna(raw_subject) and pd.notna(time):
                    cleaned_subject, type_value, presence_value, subgroups_value = extract_attributes(raw_subject)
                    data_to_insert.append(
                        (
                        cleaned_subject, classroom, time, full_date, None, type_value, presence_value, subgroups_value))
    return data_to_insert


# Нахождение всех заголовков
def find_all_headers(df, header="Преподаватель"):
    positions = []
    for col in range(df.shape[1]):
        for i, value in enumerate(df.iloc[:, col].astype(str).str.strip()):
            if value == header:
                positions.append((i, col))
    return positions


# Обработка таблицы преподавателей
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


# Сопоставление преподавателей с предметами
def get_best_match(subject, teacher_data):
    disciplines = teacher_data['Дисциплина'].unique()
    best_match, score = process.extractOne(subject, disciplines)
    return best_match if score > 60 else None


def map_teachers(schedule_data, teacher_data):
    teacher_map_by_discipline = {}
    for _, row in teacher_data.iterrows():
        discipline = row["Дисциплина"]
        teacher = row["Преподаватель"]
        if discipline not in teacher_map_by_discipline:
            teacher_map_by_discipline[discipline] = teacher

    exception_map = {
        "МПК_англ": "Межкульт_проф_комм",
        "Р_и_АТ_к_ПО": "Разр_и_ан_треб_к_ПО",
        "ОПД_зачёт": "Осн_проект_деятель",
        "БДпа": "Базы данных",
        "Опер_сист": "Операционные системы",
        "Раз_ моб_прилож": "Разработка моб_прилож",
        "Осн_инт_-технол": "Осн_интернет-технол",
        "Раз_ моб_прил": "Разработка моб_прилож",
    }

    exception_teacher_map = {}
    for exception_key, exception_value in exception_map.items():
        filtered = teacher_data[teacher_data['Дисциплина'] == exception_value]
        if not filtered.empty:
            exception_teacher_map[exception_key] = filtered['Преподаватель'].values[0]

    for idx, entry in enumerate(schedule_data):
        subject = entry[0]
        matched_discipline = get_best_match(subject, teacher_data)

        if matched_discipline:
            teacher = teacher_map_by_discipline[matched_discipline]
        elif subject in exception_teacher_map:
            teacher = exception_teacher_map[subject]
        else:
            teacher = "NA"

        schedule_data[idx] = entry[:-4] + (teacher,) + entry[-3:]


# Конвертирование времени
def convert_time_format(time_str):
    return time_str.replace('.', ':')


# Создание iCalendar файла
def create_ical_file(rows, output_path, timezone_offset):
    calendar = Calendar()
    tz = pytz.timezone(f'Etc/GMT{-timezone_offset}')

    for row in rows:
        subject, classroom, time, date_str, teacher, type_value, presence_value, subgroups_value = row

        event_name = f"{subject} ({teacher})"
        location = ", ".join(
            filter(None, [classroom, presence_value, subgroups_value]))  # Объединение, игнорируя пустые
        description = (f"Тип пары: {type_value}. "
                       f"Примечание: эта пара была создана автоматически. Возможны ошибки. Сверяйтесь!")

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
        event.description = description

        calendar.events.add(event)

    with open(output_path, 'w', encoding='utf-8') as ics_file:
        ics_file.write(calendar.serialize())

    print(f"iCalendar файл успешно создан: {output_path}")


# Получение информации о неделях для обработки
def get_weeks_to_process(total_weeks):
    week_input = input(
        f"Введите номера недель для обработки через запятую (например, 1,2) или оставьте пустым для обработки всех {total_weeks} недель: ").strip()

    if not week_input:
        return list(range(1, total_weeks + 1))  # Обработка всех недель

    # Преобразуем строку с номерами недель в список чисел
    selected_weeks = []
    try:
        selected_weeks = [int(week_num.strip()) for week_num in week_input.split(',') if week_num.strip().isdigit()]
    except ValueError:
        print("Некорректный ввод, будет обработано все расписание.")
        return list(range(1, total_weeks + 1))

    # Оставляем только те недели, которые есть в диапазоне
    return [week for week in selected_weeks if 1 <= week <= total_weeks]


# Основная функция программы
def main():
    academic_year = input("Введите учебный год (например, 2024-2025): ")
    start_date_input = input("Введите дату начала первой недели (например, 20.01): ")
    start_day, start_month = map(int, start_date_input.split('.'))
    start_year = get_year_from_month(start_month, academic_year)
    start_date = datetime(year=start_year, month=start_month, day=start_day)
    num_weeks = int(input("Введите количество учебных недель: "))

    # Получение списка недель для обработки
    weeks_to_process = get_weeks_to_process(num_weeks)

    file_path = "out.xlsx"
    df = load_excel_file(file_path)

    # Подключение к SQLite
    conn = sqlite3.connect("schedule.db")
    cursor = conn.cursor()
    cursor.execute(
        ''' CREATE TABLE IF NOT EXISTS schedule ( id INTEGER PRIMARY KEY AUTOINCREMENT, subject TEXT, classroom TEXT, time TEXT, date TEXT, teacher TEXT, type TEXT, presence TEXT, subgroups TEXT ) ''')

    schedule_data = process_schedule(df, start_date, weeks_to_process)

    teacher_data = process_teachers(df)
    map_teachers(schedule_data, teacher_data)
    save_schedule_to_db(conn, schedule_data)
    teacher_data.to_sql("teachers", conn, if_exists="replace", index=False)

    cursor.execute("SELECT subject, classroom, time, date, teacher, type, presence, subgroups FROM schedule")
    rows = cursor.fetchall()
    conn.close()

    output_path = "schedule.ics"
    timezone_offset = 4  # Часовой пояс +4
    create_ical_file(rows, output_path, timezone_offset)


if __name__ == '__main__':
    main()