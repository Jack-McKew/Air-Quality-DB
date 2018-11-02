import mysql.connector as sql
import pandas as pd

config = {
    'user':'root',
    'password':'AirQualityDB_2018',
    'host':'localhost',
    'database':'bom_data_test'
}


db_connection = sql.connect(**config)

df = pd.read_sql("SELECT * FROM bom_data_test.066062 LIMIT 1000",con=db_connection)

print(df)