from datetime import datetime
from ics import Calendar, Event
import sqlite3


def convert_time_format(time_str):
    # Преобразование времени из формата "HH.MM" в "HH:MM"
    return time_str.replace('.', ':')


def create_ical_file(db_path, output_path):
    # Подключение к SQLite базе данных
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Извлечение данных из таблицы schedule
    cursor.execute("SELECT subject, classroom, time, date, teacher FROM schedule")
    rows = cursor.fetchall()

    # Создание объекта календаря
    calendar = Calendar()

    # Обработка каждой строки и создание события
    for row in rows:
        subject, classroom, time, date_str, teacher = row

        # Формирование описания события
        event_name = f"{subject} {teacher}"
        location = classroom

        # Разделение времени на начало и конец, и преобразование формата времени
        start_time, end_time = map(convert_time_format, map(str.strip, time.split('-')))

        # Парсинг и форматирование даты и времени в объекты datetime
        start_datetime = datetime.strptime(f"{date_str} {start_time}", "%d.%m.%Y %H:%M")
        end_datetime = datetime.strptime(f"{date_str} {end_time}", "%d.%m.%Y %H:%M")

        # Создание события
        event = Event()
        event.name = event_name
        event.location = location
        event.begin = start_datetime
        event.end = end_datetime

        # Добавление события в календарь
        calendar.events.add(event)

    # Сохранение календаря в файл
    with open(output_path, 'w') as ics_file:
        ics_file.writelines(calendar)

    print(f"iCalendar файл успешно создан: {output_path}")


if __name__ == '__main__':
    db_path = "schedule.db"  # Путь к файлу SQLite базы данных
    output_path = "schedule.ics"  # Путь к выходному файлу iCalendar
    create_ical_file(db_path, output_path)