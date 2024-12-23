import mysql.connector

config = {
    "host": "172.29.0.5",
    "port": 3306,
    "user": "miguel",
    "password": "m",
    "database": "balance",
}

try:
    conn = mysql.connector.connect(**config)
    print("Conexión exitosa")
except mysql.connector.Error as e:
    print(f"Error: {e}")
