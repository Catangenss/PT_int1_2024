import pytest
import mysql.connector
import re
import time
import os

username_db = os.getenv('MYSQL_USER')
password_db = os.getenv('MYSQL_PASSWORD')

while True:
    try:
        conn = mysql.connector.connect(
            host='mysql_container',
            user=username_db,
            password=password_db,
            database='world',
            auth_plugin='mysql_native_password'
        )
        print("Успешное подключение к базе данных")
        conn.close()
        break
    except mysql.connector.Error as e:
        print(f"Ошибка подключения: {e}")
        time.sleep(30)  # Ожидаем 30 секунд перед следующей попыткой

time.sleep(5)

@pytest.fixture(scope='module')
def db_connection():
    conn = mysql.connector.connect(
        host='mysql_container',
        user=username_db,
        password=password_db,
        database='world',
        auth_plugin='mysql_native_password'
    )
    yield conn
    conn.close()

@pytest.mark.parametrize("like_str, result1, result2, result3", 
    [('%Republic', 100, ('RUS', 'Russian Federation'), 3), 
     ('%Monarchy%', 40, ('THA', 'Thailand'), 3), 
     ('%U_', 15, ('GIB', 'Gibraltar'), 3)])
def test_func1_no_index(db_connection, like_str, result1, result2, result3):
    cursor = db_connection.cursor()
    query = f"SELECT code, name FROM country WHERE government_form like '{like_str}';"
    cursor.execute(query)
    results = cursor.fetchall()
    assert len(results) > result1
    assert result2 in results
    for res in results:
        assert len(res[0]) == result3
    cursor.close()

@pytest.fixture
def setup_index_country_government_form(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("CREATE INDEX idx_government_form ON country(government_form);")
    db_connection.commit()
    yield
    cursor.execute("DROP INDEX idx_government_form ON country;")
    db_connection.commit()
    cursor.close()

@pytest.mark.parametrize("like_str, result1, result2, result3", 
    [('%Republic', 100, ('RUS', 'Russian Federation'), 3), 
     ('%Monarchy%', 40, ('THA', 'Thailand'), 3), 
     ('%U_', 15, ('GIB', 'Gibraltar'), 3)])
def test_func1_with_index(db_connection, setup_index_country_government_form, like_str, result1, result2, result3):
    cursor = db_connection.cursor()
    query = f"SELECT code, name FROM country WHERE government_form like '{like_str}';"
    cursor.execute(query)
    results = cursor.fetchall()
    assert len(results) > result1
    assert result2 in results
    for res in results:
        assert len(res[0]) == result3
    cursor.close()

@pytest.mark.parametrize("like_str1, like_str2, result1, result2, result3", 
    [('%I', '%V', {'Africa', 'Asia', 'Antarctica', 'Europe', 'North America', 'Oceania', 'South America'}, "Elisabeth II", True), 
     ('John%', 'Kim%', {'Africa', 'Asia', 'Oceania'}, "equality", True), 
     ('% _.%', '%-%', {'Africa', 'Asia', 'Europe', 'North America', 'Oceania'}, "George W. Bush", True)])
def test_func2_no_index(db_connection, like_str1, like_str2, result1, result2, result3):
    cursor = db_connection.cursor()
    query = f"SELECT code, name, continent, head_of_state FROM country WHERE head_of_state LIKE BINARY '{like_str1}' OR head_of_state LIKE BINARY '{like_str2}';"
    cursor.execute(query)
    results = cursor.fetchall()
    query_continents = {result[2] for result in results}
    assert query_continents == result1
    dict_leader = {}
    for res in results:
        dict_leader.setdefault(res[3], 0)
        dict_leader[res[3]] += 1
    max_count = max(dict_leader.values())
    max_keys = [key for key, value in dict_leader.items() if value == max_count]
    if len(max_keys) == 1:
        max_key = max_keys[0]
    else:
        max_key = 'equality' 
    assert max_key == result2
    for res in results:
        for item in res:
            assert item is not None
    cursor.close()

@pytest.fixture
def setup_index_country_head_of_state(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("CREATE INDEX idx_head_of_state ON country(head_of_state);")
    db_connection.commit()
    yield
    cursor.execute("DROP INDEX idx_head_of_state ON country;")
    db_connection.commit()
    cursor.close()

@pytest.mark.parametrize("like_str1, like_str2, result1, result2, result3", 
    [('%I', '%V', {'Africa', 'Asia', 'Antarctica', 'Europe', 'North America', 'Oceania', 'South America'}, "Elisabeth II", True), 
     ('John%', 'Kim%', {'Africa', 'Asia', 'Oceania'}, "equality", True), 
     ('% _.%', '%-%', {'Africa', 'Asia', 'Europe', 'North America', 'Oceania'}, "George W. Bush", True)])
def test_func2_with_index(db_connection, setup_index_country_head_of_state, like_str1, like_str2, result1, result2, result3):
    cursor = db_connection.cursor()
    query = f"SELECT code, name, continent, head_of_state FROM country WHERE head_of_state LIKE BINARY '{like_str1}' OR head_of_state LIKE BINARY '{like_str2}';"
    cursor.execute(query)
    results = cursor.fetchall()
    query_continents = {result[2] for result in results}
    assert query_continents == result1
    dict_leader = {}
    for res in results:
        dict_leader.setdefault(res[3], 0)
        dict_leader[res[3]] += 1
    max_count = max(dict_leader.values())
    max_keys = [key for key, value in dict_leader.items() if value == max_count]
    if len(max_keys) == 1:
        max_key = max_keys[0]
    else:
        max_key = 'equality' 
    assert max_key == result2
    for res in results:
        for item in res:
            assert item is not None
    cursor.close()

@pytest.mark.parametrize("like_str, result1, result2, result3", 
    [('%(%)', 'IND', 10000000, 'Hinthada'), 
     ('%-%', 'RUS', 1000000, 'Rostov'), 
     ('%sk', 'RUS', 1500000, 'Omsk')])
def test_func3_no_index(db_connection, like_str, result1, result2, result3):
    cursor = db_connection.cursor()
    query = f"SELECT * FROM city WHERE name LIKE '{like_str}' ORDER BY population DESC;"
    cursor.execute(query)
    results = cursor.fetchall()
    country_leader = {}
    for res in results:
        country_leader.setdefault(res[2], 0)
        country_leader[res[2]] += 1
    max_count = max(country_leader.values())
    max_keys = [key for key, value in country_leader.items() if value == max_count]
    if len(max_keys) == 1:
        max_key = max_keys[0]
    else:
        max_key = 'equality' 
    assert max_key == result1
    assert results[0][4] > result2
    flag = any(result3 in res[1] for res in results)
    assert flag == True
    cursor.close()

@pytest.fixture
def setup_index_city_name(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("CREATE INDEX idx_city_name ON city(name);")
    db_connection.commit()
    yield
    cursor.execute("DROP INDEX idx_city_name ON city;")
    db_connection.commit()
    cursor.close()

@pytest.mark.parametrize("like_str, result1, result2, result3", 
    [('%(%)', 'IND', 10000000, 'Hinthada'), 
     ('%-%', 'RUS', 1000000, 'Rostov'), 
     ('%sk', 'RUS', 1500000, 'Omsk')])
def test_func3_with_index(db_connection, setup_index_city_name, like_str, result1, result2, result3):
    cursor = db_connection.cursor()
    query = f"SELECT * FROM city WHERE name LIKE '{like_str}' ORDER BY population DESC;"
    cursor.execute(query)
    results = cursor.fetchall()
    country_leader = {}
    for res in results:
        country_leader.setdefault(res[2], 0)
        country_leader[res[2]] += 1
    max_count = max(country_leader.values())
    max_keys = [key for key, value in country_leader.items() if value == max_count]
    if len(max_keys) == 1:
        max_key = max_keys[0]
    else:
        max_key = 'equality' 
    assert max_key == result1
    assert results[0][4] > result2
    flag = any(result3 in res[1] for res in results)
    assert flag == True
    cursor.close()

@pytest.mark.parametrize("like_str, result1, result2, result3", 
    [('%chinese', 3, 3, 3), 
     ('%french', 4, 18, 6), 
     ('%ussian', 3, 4, 1)])
def test_func4_no_index(db_connection, like_str, result1, result2, result3):
    cursor = db_connection.cursor()
    query = f"SELECT * FROM country_language WHERE language like '{like_str}' ORDER BY percentage DESC;"
    cursor.execute(query)
    results = cursor.fetchall()
    languages = {res[1] for res in results}
    assert len(languages) == result1
    official_language = sum(1 for res in results if res[2] == "T")
    assert official_language == result2
    language_skills = sum(1 for res in results if res[3] >= 80)
    assert language_skills == result3
    cursor.close()

@pytest.fixture
def setup_index_country_language_language(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("CREATE INDEX idx_language ON country_language(language);")
    db_connection.commit()
    yield
    cursor.execute("DROP INDEX idx_language ON country_language;")
    db_connection.commit()
    cursor.close()

@pytest.mark.parametrize("like_str, result1, result2, result3", 
    [('%chinese', 3, 3, 3), 
     ('%french', 4, 18, 6), 
     ('%ussian', 3, 4, 1)])
def test_func4_with_index(db_connection, setup_index_country_language_language, like_str, result1, result2, result3):
    cursor = db_connection.cursor()
    query = f"SELECT * FROM country_language WHERE language like '{like_str}' ORDER BY percentage DESC;"
    cursor.execute(query)
    results = cursor.fetchall()
    languages = {res[1] for res in results}
    assert len(languages) == result1
    official_language = sum(1 for res in results if res[2] == "T")
    assert official_language == result2
    language_skills = sum(1 for res in results if res[3] >= 80)
    assert language_skills == result3
    cursor.close()

@pytest.mark.parametrize("like_str, result1, result2, result3", 
    [('South%', 'Argentina', False, True), 
     ('% % %', 'Kazakstan', True, False), 
     ('Nor%', 'Finland', False, True)])
def test_func5_no_index(db_connection, like_str, result1, result2, result3):
    cursor = db_connection.cursor()
    query = f"SELECT country.code, country.name AS country_name, country.region, city.name AS capital_name, country.population, city.population AS capital_population FROM country JOIN city ON country.capital = city.id WHERE region LIKE '{like_str}';"
    cursor.execute(query)
    results = cursor.fetchall()
    assert any(res[1] == result1 for res in results)
    pattern = r'^[A-Za-z ]+$'
    assert all(re.fullmatch(pattern, res[3]) is not None for res in results) == result2
    assert any(res[5] / res[4] >= 0.8 for res in results) == result3
    cursor.close()

@pytest.fixture
def setup_index_country_region(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("CREATE INDEX idx_region ON country(region);")
    db_connection.commit()
    yield
    cursor.execute("DROP INDEX idx_region ON country;")
    db_connection.commit()
    cursor.close()

@pytest.mark.parametrize("like_str, result1, result2, result3", 
    [('South%', 'Argentina', False, True), 
     ('% % %', 'Kazakstan', True, False), 
     ('Nor%', 'Finland', False, True)])
def test_func5_with_index(db_connection, setup_index_country_region, like_str, result1, result2, result3):
    cursor = db_connection.cursor()
    query = f"SELECT country.code, country.name AS country_name, country.region, city.name AS capital_name, country.population, city.population AS capital_population FROM country JOIN city ON country.capital = city.id WHERE region LIKE '{like_str}';"
    cursor.execute(query)
    results = cursor.fetchall()
    assert any(res[1] == result1 for res in results)
    pattern = r'^[A-Za-z ]+$'
    assert all(re.fullmatch(pattern, res[3]) is not None for res in results) == result2
    assert any(res[5] / res[4] >= 0.8 for res in results) == result3
    cursor.close()


# Подключаемся к новой БД для тестов производительности
@pytest.fixture(scope='module')
def db_connection_perf():
    conn = mysql.connector.connect(
        host='mysql_container',
        user=username_db,
        password=password_db,
        database='employees',
        auth_plugin='mysql_native_password'
    )
    yield conn
    conn.close()


@pytest.mark.parametrize("like_str", ['Ne%', 'Nev%', 'Nevi%'])
def test_performance_success(db_connection_perf, like_str):
    cursor = db_connection_perf.cursor()

    query = f"""
        SELECT e.first_name, e.last_name, e.birth_date, e.gender, e.hire_date, t.title, t.from_date
        FROM employees AS e
        JOIN titles AS t ON e.emp_no = t.emp_no
        WHERE t.to_date = DATE('9999-01-01') AND e.first_name LIKE '{like_str}';
    """

    # Время выполнения запроса без индекса
    start_time = time.time()
    cursor.execute(query)
    cursor.fetchall()
    end_time = time.time()
    execution_time_without_index = end_time - start_time
    print(f"\nВремя выполнения без индекса: {execution_time_without_index:.6f} секунд")

    # Добавляем индекс
    cursor.execute("SHOW INDEX FROM employees WHERE Key_name = 'firstname_idx';")
    index_exists = cursor.fetchone()
    if not index_exists:
        cursor.execute("CREATE INDEX firstname_idx ON employees(first_name);")
        db_connection_perf.commit()

    # Время выполнения запроса с индексом
    start_time = time.time()
    cursor.execute(query)
    cursor.fetchall()
    end_time = time.time()
    execution_time_with_index = end_time - start_time
    print(f"Время выполнения с индексом: {execution_time_with_index:.6f} секунд")

    # Рассчитываем разницу во времени выполнения и коэффициент ускорения
    time_difference = execution_time_without_index - execution_time_with_index
    speedup_factor = execution_time_without_index / execution_time_with_index

    print(f"Разница во времени выполнения: {time_difference:.6f} секунд")
    print(f"Ускорение с индексом: {speedup_factor:.2f} раз")

    # Удаляем индекс
    cursor.execute("DROP INDEX firstname_idx ON employees;")
    db_connection_perf.commit()
    cursor.close()


    # Проверяем, что запрос с индексом быстрее
    assert execution_time_with_index < execution_time_without_index


@pytest.mark.parametrize("like_str", ['%k%', '%k', '_ke__'])
def test_performance_fail(db_connection_perf, like_str):
    cursor = db_connection_perf.cursor()

    query = f"""
        SELECT e.first_name, e.last_name, e.birth_date, e.gender, e.hire_date, t.title, t.from_date
        FROM employees AS e
        JOIN titles AS t ON e.emp_no = t.emp_no
        WHERE t.to_date = DATE('9999-01-01') AND e.first_name LIKE '{like_str}';
    """
    # Время выполнения запроса без индекса
    start_time = time.time()
    cursor.execute(query)
    cursor.fetchall()
    end_time = time.time()
    execution_time_without_index = end_time - start_time
    print(f"\nВремя выполнения без индекса: {execution_time_without_index:.6f} секунд")

    # Включаем индекс и запускаем тест ещё раз
    cursor.execute("SHOW INDEX FROM employees WHERE Key_name = 'firstname_idx';")
    index_exists = cursor.fetchone()
    if not index_exists:
        cursor.execute("CREATE INDEX firstname_idx ON employees(first_name);")
        db_connection_perf.commit()

    # Время выполнения запроса с индексом
    start_time = time.time()
    cursor.execute(query)
    cursor.fetchall()
    end_time = time.time()
    execution_time_with_index = end_time - start_time
    print(f"Время выполнения с индексом: {execution_time_with_index:.6f} секунд")

    # Рассчитываем разницу во времени выполнения и коэффициент ускорения
    time_difference = execution_time_without_index - execution_time_with_index
    speedup_factor = execution_time_without_index / execution_time_with_index

    print(f"Разница во времени выполнения: {time_difference:.6f} секунд")
    print(f"Ускорение с индексом: {speedup_factor:.2f} раз")
    print(f"Для оператора LIKE '{like_str}' запрос выполняется без индекса")

    # Удаляем индекс
    cursor.execute("DROP INDEX firstname_idx ON employees;")
    db_connection_perf.commit()
    cursor.close()


@pytest.mark.parametrize("like_str", ['E%', 'En%', 'Eng%'])
def test_performance_fail_2(db_connection_perf, like_str):
    cursor = db_connection_perf.cursor()

    print("\nПроверка производительности для столбца с низкой селективностью - titles.title")

    query = f"""
        SELECT e.first_name, e.last_name, e.birth_date, e.gender, e.hire_date, t.title, t.from_date
        FROM employees AS e
        JOIN titles AS t ON e.emp_no = t.emp_no
        WHERE t.to_date = DATE('9999-01-01') AND t.title LIKE '{like_str}';
    """
    # Время выполнения запроса без индекса
    start_time = time.time()
    cursor.execute(query)
    cursor.fetchall()
    end_time = time.time()
    execution_time_without_index = end_time - start_time
    print(f"Время выполнения без индекса: {execution_time_without_index:.6f} секунд")

    # Включаем индекс и запускаем тест ещё раз
    cursor.execute("SHOW INDEX FROM titles WHERE Key_name = 'title_idx';")
    index_exists = cursor.fetchone()
    if not index_exists:
        cursor.execute("CREATE INDEX title_idx ON titles(title);")
        db_connection_perf.commit()

    # Время выполнения запроса с индексом
    start_time = time.time()
    cursor.execute(query)
    cursor.fetchall()
    end_time = time.time()
    execution_time_with_index = end_time - start_time
    print(f"Время выполнения с индексом: {execution_time_with_index:.6f} секунд")

    # Рассчитываем разницу во времени выполнения и коэффициент ускорения
    time_difference = execution_time_without_index - execution_time_with_index
    speedup_factor = execution_time_without_index / execution_time_with_index

    print(f"Разница во времени выполнения: {time_difference:.6f} секунд")
    print(f"Ускорение с индексом: {speedup_factor:.2f} раз")
    print(f"Для оператора LIKE '{like_str}' запрос выполняется без индекса")

    # Удаляем индекс
    cursor.execute("DROP INDEX title_idx ON titles;")
    db_connection_perf.commit()
    cursor.close()
