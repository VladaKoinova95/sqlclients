import psycopg2


def drop_db(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS phones;")
        cur.execute("DROP TABLE IF EXISTS emails;")
        cur.execute("DROP TABLE IF EXISTS clients;")
    conn.commit()


# Функция, создающая структуру БД (таблицы)
def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clients (
        	    client_id SERIAL PRIMARY KEY,
        	    first_name VARCHAR(50) NOT NULL,
        	    last_name VARCHAR(50) NOT NULL
            );
            """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS phones (
            	phone_id SERIAL PRIMARY KEY,
            	client_id INTEGER NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
            	phone VARCHAR(50)
            );
            """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS emails (
                email_id SERIAL PRIMARY KEY,
                client_id INTEGER NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
                email VARCHAR(50)
            );
            """)
        conn.commit()


# Функция, проверяющая наличие ID таблице Clients
def check_id(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT * FROM clients
            WHERE client_id = %s;
        """, (client_id,))
        return cur.fetchone()


# Функция, позволяющая добавить нового клиента
def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clients (first_name, last_name) 
            VALUES  (%s,%s)
            RETURNING client_id, first_name;
            """, (first_name, last_name))
        client_id = cur.fetchone()[0]
        print(f"Добавлена запись в таблицу Clients: ID {client_id}, {first_name} {last_name}")
        cur.execute("""
            INSERT INTO emails (client_id,email) 
            VALUES  (%s,%s)
            RETURNING email_id;
            """, (client_id, email))
        email_id = cur.fetchone()[0]
        print(f"Добавлена запись в таблицу Emails: ID {email_id}, {email}")
        if phones:
            for phone in phones:
                cur.execute("""
                    INSERT INTO phones (client_id,phone) 
                    VALUES (%s,%s)
                    RETURNING phone_id;
                    """, (client_id, phone))
                phone_id = cur.fetchone()[0]
                print(f"Добавлена запись в таблицу Phones: ID {phone_id}, {phone}")


# Функция, позволяющая добавить телефон для существующего клиента
def add_phone(conn, client_id, phone):
    if check_id(conn, client_id):
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO phones (client_id, phone)
                VALUES (%s,%s)
                RETURNING phone_id;
            """, (client_id, phone))
            phone_id = cur.fetchone()[0]
            print(f"Добавлена запись в таблицу Phones: ID {phone_id}, {phone}")
    else:
        print(f"Клиента с ID {client_id} не существует")

# Функция, позволяющая изменить данные о клиенте
def change_client(conn, client_id, first_name=None, last_name=None, email=None, phone_id=None, phone=None):
    if check_id(conn, client_id):
        with conn.cursor() as cur:
            if first_name or last_name:
                cur.execute("""
                    UPDATE clients 
                    SET first_name = COALESCE(%s, first_name),
                    last_name = COALESCE(%s, last_name)
                    WHERE client_id = %s
                    RETURNING first_name, last_name;
                """, (first_name, last_name, client_id))
                if cur.rowcount:
                    print(f"Обновлена запись в таблице Clients: ID {client_id}, {cur.fetchone()}")
            if email:
                cur.execute("""
                    UPDATE emails
                    SET email = %s
                    WHERE client_id = %s
                    RETURNING email_id;
                """, (email, client_id))
                if cur.rowcount:
                    print(f"Обновлена запись в таблице Emails: ID {cur.fetchone()[0]}, {email}")
            if phone_id and phone:
                cur.execute("""
                    UPDATE phones
                    SET phone = %s
                    WHERE phone_id = %s AND client_id = %s;
                """, (phone, phone_id,client_id))
                if cur.rowcount:
                    print(f"Обновлена запись в таблице Phones: ID {phone_id}, {phone}")
    else:
        print(f"Клиента с ID {client_id} не существует")


# Функция, позволяющая удалить телефон для существующего клиента
def delete_phone(conn, client_id, phone):
    if check_id(conn, client_id):
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM phones
                WHERE client_id = %s AND phone = %s;
            """, (client_id, phone))
            for el in cur.fetchall():
                cur.execute("""
                    DELETE FROM phones
                    WHERE phone_id = %s;
                """, (el[0],))
                print(f"Удалена запись в таблице Phones: ID {el[0]},{phone}")
    else:
        print(f"Клиента с ID {client_id} не существует")


# Функция, позволяющая удалить существующего клиента
def delete_client(conn, client_id):
    if check_id(conn, client_id):
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM clients
                WHERE client_id = %s;
            """, (client_id,))
        conn.commit()
        print(f"Удалена информация в БД по клиенту: {client_id}")
    else:
        print(f"Клиента с ID {client_id} не существует")


# Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону
def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    where_values = []
    values = []
    if first_name:
        where_values.append('first_name = %s')
        values.append(first_name)
    if last_name:
        where_values.append('last_name = %s')
        values.append(last_name)
    if email:
        where_values.append('email = %s')
        values.append(email)
    if phone:
        where_values.append("phone = %s")
        values.append(phone)
    if len(where_values) == 0:
        return []
    query = (f"""
        SELECT DISTINCT c.client_id, first_name, last_name FROM clients c
        LEFT JOIN emails e on c.client_id = e.client_id
        LEFT JOIN phones p on c.client_id = p.client_id
        WHERE {' AND '.join(where_values)}
    """)
    with conn.cursor() as cur:
        cur.execute(query, tuple(values))
        rows = cur.fetchall()
        return rows


with psycopg2.connect(database="clients_db", user="postgres", password="461995") as conn:
    drop_db(conn)
    create_db(conn)
    add_client(conn,"Влада", "Коинова", "vlada@mail.ru", ["543211","89179172200"])
    add_client(conn,"Андрей", "Иванов", "ivanov@yandex.ru", ["89603204568"])
    add_client(conn, "Андрей", "Некрасов", "an1994@mail.ru")
    add_client(conn, "Оля", "Краснова", "olya@mail.ru", ["777"])
    add_client(conn, "Михаил", "Ибрагимов", "mi_boss@mail.ru")
    add_phone(conn, "2", "543264")
    add_phone(conn, "3", "89176204532")
    delete_phone(conn, "4", "777")
    delete_client(conn, "5")
    delete_client(conn, "8")
    change_client(conn, "4", last_name="Воронина", email="voroninaolya@mail.ru")
    change_client(conn, "2", phone_id="3", phone="89603204567")
    print(find_client(conn, first_name="Андрей"))
    print(find_client(conn, email="vlada@mail.ru"))
    print(find_client(conn, first_name="Андрей", phone="543264"))
    print(find_client(conn, first_name="Марина"))
conn.close()
