import mysql.connector
from mysql.connector import Error
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def fetch_and_display_table(connection, table_name):
    cursor = connection.cursor()
    try:
        # Primeiro, pegamos os nomes das colunas
        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        columns = [column[0] for column in cursor.fetchall()]
        
        # Depois, buscamos os dados
        cursor.execute(f"SELECT * FROM {table_name}")
        results = cursor.fetchall()
        
        # Criamos um DataFrame com os resultados
        df = pd.DataFrame(results, columns=columns)
        print("\nDados da tabela:")
        print(df)
        return df
    except Error as err:
        print(f"Error: '{err}'")
        return None

# # Use o código assim:
# connection = create_server_connection(db_host, db_user, db_pass)
# cursor = connection.cursor()
# cursor.execute("USE agenciavfx")
# df = fetch_and_display_table(connection, "oportunidades")



# connection = create_server_connection(db_host, db_user, db_pass)
# 
# connection._execute_query("use agenciavfx")
# connection._execute_query("select * from oportunidades")
# 
# 
# # connection._execute_query("use agenciavfx")
# # connection._execute_query(f'''insert into oportunidades(id, nome, valor)
# #                           values(1, 'Lucas Baruffi - Agência VFX', 125000)''')
# connection.commit()
