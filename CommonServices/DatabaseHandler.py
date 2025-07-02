import mysql.connector
from mysql.connector import Error

class DatabaseHandler:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.connection.is_connected():
                print("Connected to MySQL database")
        except Error as e:
            print(f"Error while connecting to MySQL: {e}")

    def disconnect(self):
        if self.connection.is_connected():
            self.connection.close()
            print("MySQL connection closed")

    def execute_query(self, query, params=None):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            self.connection.commit()
            print("Query executed successfully")
        except Error as e:
            print(f"Error executing query: {e}")
        finally:
            cursor.close()

    def fetch_all(self, query, params=None):
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query, params)
            result = cursor.fetchall()
            return result
        except Error as e:
            print(f"Error fetching data: {e}")
            return None
        finally:
            cursor.close()

    def fetch_one(self, query, params=None):
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query, params)
            result = cursor.fetchone()
            return result
        except Error as e:
            print(f"Error fetching data: {e}")
            return None
        finally:
            cursor.close()

    def close(self):
        if self.connection.is_connected():
            self.connection.close()
            print("Connection to MySQL closed")

# Usage example
if __name__ == "__main__":
    db_handler = DatabaseHandler(host="localhost", user="root", password="password", database="mydb")
    db_handler.connect()

    # Example of executing a query
    create_table_query = """
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100),
        email VARCHAR(100),
        age INT
    )
    """
    db_handler.execute_query(create_table_query)

    # Example of inserting data
    insert_query = "INSERT INTO users (name, email, age) VALUES (%s, %s, %s)"
    db_handler.execute_query(insert_query, ("John Doe", "john@example.com", 30))

    # Example of fetching data
    select_query = "SELECT * FROM users"
    rows = db_handler.fetch_all(select_query)
    for row in rows:
        print(row)

    # Close the connection
    db_handler.disconnect()
