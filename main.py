import psycopg2
import datetime


def get_connection():
    conn = psycopg2.connect(
        host='127.0.0.1', user='root', password='mypass',
        port='5432', database='example',
    )
    conn.autocommit = True
    cursor = conn.cursor()
    return conn, cursor


def close_connection():
    cursor.close()
    conn.close()


def create_db():
    global conn
    global cursor
    conn, cursor = get_connection()
    cursor.execute('select version()')
    print(cursor.fetchone())
    sql = """
        CREATE TABLE IF NOT EXISTS COURSES (
            id SERIAL PRIMARY KEY,
            name char(100) NOT NULL
        );
        CREATE TABLE IF NOT EXISTS STUDENTS (
            id SERIAL,
            course_id INTEGER REFERENCES COURSES(id),
            name char(100) NOT NULL,
            gpa numeric(10,2),
            birth timestamptz,
            PRIMARY KEY (id, course_id)
        );
    """
    cursor.execute(sql)


def add_course(course_name):
    sql = 'insert into courses (name) values (%s)'
    cursor.execute(sql, (course_name, ))


def add_students(course_id, students_data_list):
    for student_data in students_data_list:
        add_student(course_id, student_data)


def add_student(course_id, student_dict):
    name = student_dict.get('name')
    gpa = student_dict.get('gpa')
    birth = student_dict.get('birth')
    sql = '''
    insert into students (course_id, name, gpa, birth)
    values (%s, %s, %s, %s)
    '''
    cursor.execute(sql, [course_id, name, gpa, birth])


def get_courses():
    sql = 'select * from courses'
    cursor.execute(sql)
    return cursor.fetchall()


def get_students(course_id):
    cursor.execute('select * from students where course_id = %s', [course_id])
    return cursor.fetchall()


def get_student(student_id):
    cursor.execute('select * from students where id=%s', [student_id])
    return cursor.fetchall()


if __name__ == '__main__':

    now_with_tz = datetime.datetime.utcnow()

    students_python = [
        {'name': 'Ivan', 'gpa': 1, 'birth': now_with_tz},
        {'name': 'Peter', 'gpa': 2, 'birth': now_with_tz},
        {'name': 'Haudy', 'gpa': 20, 'birth': now_with_tz},
    ]

    students_cpp = [
        {'name': 'Fred', 'gpa': 1, 'birth': now_with_tz},
        {'name': 'Fnank', 'gpa': 1, 'birth': now_with_tz},
    ]

    create_db()
    add_course('Python')
    add_course('C++')

    print(get_courses())

    add_students(1, students_python)
    add_students(2, students_cpp)

    print(get_students(1))
    print(get_students(2))

    print(get_student(1))

    close_connection()
