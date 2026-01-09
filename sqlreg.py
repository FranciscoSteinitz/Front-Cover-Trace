import mysql.connector
from mysql.connector import Error

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("✅ MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
    return connection

def create_database(connection, query,dbname:str):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.database = dbname
        print("✅ Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

def show_table_query(host_name, user_name, user_password, data_base_name, table_name):
    
    connection = create_server_connection(host_name, user_name, user_password)
    print(connection)
    create_database_query = f"CREATE DATABASE IF NOT EXISTS {data_base_name}"
    create_database(connection, create_database_query, data_base_name)

    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM {table_name}")
        result = cursor.fetchall() #  Contiene todas las filas de la tabla, lista de tuplas
        # Imprime todas las filas de la tabla de forma legible, se puede comentar
        #for row in result:
            #print(row)
        connection.close()
    except Error as err:
        connection.close()
        result = err
        print(f"Error: '{err}'")
    return result

def insert_data(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Data inserted successfully")
    except Error as err:
        print(f"Error: '{err}'")

def search_data(connection, table, query, criteria):
    #Inicializar variable ID
    ID = []
    data_filter = f"""
        SELECT ID, {query}
        FROM {table}
        WHERE {query} = "{criteria}"
        ORDER BY ID DESC
        """
    try:
        with connection.cursor() as cursor:
            cursor.execute(data_filter)
            for ID in cursor.fetchall():
                print(ID)
        if len(ID) == 1:
            return 1
        if len(ID) > 1:
            return 2
    except Exception as e:
        return 3
    
def update_data(connection, table, dest_query, new_criteria, query, criteria ):
    update_query = f"""
    UPDATE
        {table}
    SET
        {dest_query} = "{new_criteria}"
    WHERE
        {query} = {criteria}
    """
    with connection.cursor() as cursor:
        cursor.execute(update_query)
        connection.commit()

def update_part_data(connection, nombre_tabla_sql:str, decode_data:dict):
    try:
        cursor = connection.cursor()
        set_clause = ', '.join([f"{key} = %s" for key in decode_data.keys() if key != 'SerialNumber'])
        sql = f"UPDATE {nombre_tabla_sql} SET {set_clause} WHERE SerialNumber = %s"
        valores = [value for key, value in decode_data.items() if key != 'SerialNumber']
        valores.append(decode_data['SerialNumber'])
        cursor.execute(sql, valores)
        connection.commit()
    #Error de dato serial no existente
    except mysql.connector.Error as error:
        print(f'Error al actualizar en MYSQL: {error}')
        
def delete_data(connection, table, query, criteria):
    id_list = search_data(connection, table, query, criteria)
    print(f"ID List Length: {id_list}")
    if id_list == 2 :
        delete_query = f"DELETE FROM {table} WHERE {query} = {criteria}"
        with connection.cursor() as cursor:
            cursor.execute(delete_query)
            connection.commit()

def recv_Data_To_MySQL(connection, nombre_tabla_sql:str, decode_data:dict):
    try:
        cursor = connection.cursor()
        columnas = ', '.join(decode_data.keys())
        valores_placeholder = ', '.join(['%s'] * len(decode_data))
        sql = f"INSERT into {nombre_tabla_sql} ({columnas}) VALUES ({valores_placeholder})"
        valores = list(decode_data.values())
        cursor.execute(sql, valores)
        connection.commit()
    except mysql.connector.Error as error:
        print(f'Error al insertar en MYSQL: {error}')

def get_Data_From_MySQL(connection, nombre_tabla_sql:str, decode_data:dict, criteria_value):
    try:
        cursor = connection.cursor()
        sql = f"SELECT * FROM {nombre_tabla_sql} WHERE SerialNumber = %s"
        cursor.execute(sql, (criteria_value,))
        result = cursor.fetchone()
        if result:
            columnas = [desc[0] for desc in cursor.description]
            for i, columna in enumerate(columnas):
                decode_data[columna] = result[i]
    except mysql.connector.Error as error:
        print(f'Error al obtener datos de MYSQL: {error}')

# if __name__ == "__main__":
#     #Parametros de conexion
#     host_name = "localhost"
#     user_name = "root"
#     user_password = "acme2019"
#     data_base_name = "plc_data"
#     nombre_tabla_sql = "front_cover"

#     #Mostrar tabla con conexion y desconexion dentro de la funcion, la funcion retorna el resultado en forma de lista de tuplas
#     print(show_table_query(host_name, user_name, user_password, data_base_name, nombre_tabla_sql))



