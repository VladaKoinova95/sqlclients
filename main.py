import psycopg2

def drop_db(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS phones;")
        cur.execute("DROP TABLE IF EXISTS emails;")
        cur.execute("DROP TABLE IF EXISTS clients;")
    conn.commit()

#Функция, создающая структуру БД (таблицы)
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
            	client_id INTEGER NOT NULL REFERENCES clients(client_id),
            	phone VARCHAR(50)
            );
            """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS emails (
                email_id SERIAL PRIMARY KEY,
                client_id INTEGER NOT NULL REFERENCES clients(client_id),
                email VARCHAR(50)
            );
            """)
        conn.commit()

with psycopg2.connect(database="clients_db", user="postgres", password="461995") as conn:
    drop_db(conn)
    create_db(conn)
conn.close()