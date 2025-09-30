import os
import csv
import mysql.connector
import boto3

# Variables de entorno
host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")
table = os.getenv("DB_TABLE")
bucket = os.getenv("S3_BUCKET", "ktamayo-s2")
output_file = os.getenv("OUTPUT_FILE", "data.csv")

# Conectar MySQL
conn = mysql.connector.connect(
    host=host, user=user, password=password, database=database
)
cursor = conn.cursor()
cursor.execute(f"SELECT * FROM {table}")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]

# Guardar CSV
with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(columns)
    writer.writerows(rows)

cursor.close()
conn.close()

# Subir a S3
s3 = boto3.client("s3")
s3.upload_file(output_file, bucket, output_file)

print("Ingesta completada en s3://{}/{}".format(bucket, output_file))
