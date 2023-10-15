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
        print(f"Добавлена запись в таблицу Clients: ID {client_id}")
        cur.execute("""
            INSERT INTO emails (client_id,email) 
            VALUES  (%s,%s)
            RETURNING email_id;
            """, (client_id, email))
        email_id = cur.fetchone()[0]
        print(f"Добавлена запись в таблицу Emails: ID {email_id}")
        if phones:
            for phone in phones:
                cur.execute("""
                    INSERT INTO phones (client_id,phone) 
                    VALUES (%s,%s)
                    RETURNING phone_id;
                    """, (client_id, phone))
                phone_id = cur.fetchone()[0]
                print(f"Добавлена запись в таблицу Phones: ID {phone_id}")


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
            print(f"Добавлена запись в таблицу Phones: ID {phone_id}")
    else:
        print(f"Клиента с ID {client_id} не существует")


#Функция, позволяющая удалить телефон для существующего клиента
def delete_phone(conn, client_id, phone):
    if check_id(conn, client_id):
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM phones
                WHERE client_id = %s AND phone = %s;
            """, (client_id, phone))
            print(cur.fetchall())
    else:
        print(f"Клиента с ID {client_id} не существует")

#Функция, позволяющая удалить существующего клиента
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


with psycopg2.connect(database="clients_db", user="postgres", password="461995") as conn:
    drop_db(conn)
    create_db(conn)
    add_client(conn, "Влада", "Коинова", "vlada@mail.ru", ["88552543264", "88552543264"])
    add_client(conn, "Роман", "Блинов", "oleg@mail.ru", ["589646"])
    delete_client(conn, "1")
    #add_phone(conn, "2", "5")
conn.close()
