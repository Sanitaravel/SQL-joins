import sqlite3
import prettytable


def create_connection(db_file):
    """ Create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)

    return conn


def select_all_executor(conn: sqlite3.Connection):
    """
    Query all workers, who has destinations
    :param conn: the Connection object
    :return: None
    """
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT t1.first_name, t1.second_name "
                "FROM workers t1 "
                "INNER JOIN destinatons t2 "
                "ON t1.worker_id = t2.worker_id")

    output = prettytable.from_db_cursor(cur)
    print(output)


def select_all_2020_task(conn: sqlite3.Connection):
    """
    Query all task, which was made in 2020
    :param conn: the Connection object
    :return: None
    """
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT task_id, name FROM tasks "
                    f"WHERE created_at >= date('2020-01-01') "
                    f"AND created_at <= date('2020-31-12')")
    except sqlite3.Error as e:
        print(e)

    output = prettytable.from_db_cursor(cur)
    print(output)


def select_all_task(conn: sqlite3.Connection):
    """
    Query all tasks
    :param conn: the Connection object
    :return: None
    """
    cur = conn.cursor()
    try:
        cur.execute("SELECT tasks.task_id, tasks.name, task_type.task_type_name FROM tasks "
                    "JOIN task_type "
                    "ON tasks.task_type_id = task_type.task_type_id")
    except sqlite3.Error as e:
        print(e)

    output = prettytable.from_db_cursor(cur)
    print(output)


def select_repair(conn: sqlite3.Connection):
    """
    Query all workers, who has job "repair"
    :param conn: the Connection object
    :return: None
    """
    cur = conn.cursor()
    try:
        cur.execute("SELECT W.first_name, W.second_name, Ta.name FROM destinatons D "
                    "INNER JOIN tasks Ta ON Ta.task_id = D.task_id AND Ta.task_type_id = 5 "
                    "INNER JOIN workers W ON W.worker_id = D.worker_id")
    except sqlite3.Error as e:
        print(e)

    output = prettytable.from_db_cursor(cur)
    print(output)


def select_none_0_worker_task(conn: sqlite3.Connection):
    """
    Query all task, which have at least one executor
    :param conn: the Connection object
    :return: None
    """
    cur = conn.cursor()
    try:
        cur.execute("SELECT DISTINCT Ta.name FROM destinatons D "
                    "INNER JOIN tasks Ta ON Ta.task_id = D.task_id ")
    except sqlite3.Error as e:
        print(e)

    output = prettytable.from_db_cursor(cur)
    print(output)


def main():
    conn = create_connection('db.sqlite')
    print("""
Добро пожаловать в нашу базу данных.
Для получения данных используйте следующую таблицу:
Код    Задание
help   Получить все команды
1      Получить список всех исполнителей
2      Получить список заданий, созданных в 2020 году
3      Получить список всех заданий
4      Получить список имён всех исполнителей задач типа "ремонт" с указанием задачи, которую они выполняют.
5      Получить список всех задач, которым назначен хоть один исполнитель.
0      Выйти из программы
    """)
    command = input("Введите команду: ")
    while command != "0":
        if command == "help":
            print("""
Для получения данных используйте следующую таблицу:
Код    Задание
help   Получить все команды
1      Получить список всех исполнителей
2      Получить список заданий, созданных в 2020 году
3      Получить список всех заданий
4      Получить список имён всех исполнителей задач типа "ремонт" с указанием задачи, которую они выполняют.
5      Получить список всех задач, которым назначен хоть один исполнитель.
0      Выйти из программы""")
        elif command == "1":
            select_all_executor(conn)
        elif command == "2":
            select_all_2020_task(conn)
        elif command == "3":
            select_all_task(conn)
        elif command == "4":
            select_repair(conn)
        elif command == "5":
            select_none_0_worker_task(conn)
        else:
            print("Неправильная команда!")
        command = int(input("\nВведите команду: "))
    print("До новых встреч!")


if __name__ == '__main__':
    main()
