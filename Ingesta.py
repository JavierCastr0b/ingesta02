import mysql.connector
import csv
import boto3

# =========================
# CONFIGURACIÓN MYSQL
# =========================

host_name = "172.31.27.164"      # IPv4 privada de la MV Base de Datos
port_number = 8005               # Puerto expuesto del contenedor mysql_c
user_name = "root"
password_db = "utec"
database_name = "bd_api_employees"
table_name = "employees"

# =========================
# CONFIGURACIÓN S3
# =========================

bucket_name = "jcb-output-01"
csv_file = "employees.csv"
s3_key = "ingesta02/employees.csv"

try:
    mydb = mysql.connector.connect(
        host=host_name,
        port=port_number,
        user=user_name,
        password=password_db,
        database=database_name
    )

    print("Conexión a MySQL exitosa")

    cursor = mydb.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")

    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]

    print(f"Registros leídos desde MySQL: {len(rows)}")

    with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows(rows)

    print(f"Archivo CSV generado correctamente: {csv_file}")

    s3 = boto3.client("s3")
    s3.upload_file(csv_file, bucket_name, s3_key)

    print("Ingesta completada")
    print(f"Archivo subido correctamente a s3://{bucket_name}/{s3_key}")

except mysql.connector.Error as error:
    print("Error al conectar o consultar MySQL:")
    print(error)

except Exception as error:
    print("Error general:")
    print(error)

finally:
    if "cursor" in locals():
        cursor.close()

    if "mydb" in locals() and mydb.is_connected():
        mydb.close()
        print("Conexión MySQL cerrada")
