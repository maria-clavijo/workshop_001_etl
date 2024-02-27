import configparser
import mysql.connector
import pandas as pd


config = configparser.ConfigParser()
config.read('db_config.ini')

host = config['mysql']['host']
user = config['mysql']['user']
password = config['mysql']['password']


# Connect to the MySQL database
connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password
)

print("¡Connected to the database!")
cursor = connection.cursor()

# Create the database in MySQL
cursor.execute("CREATE DATABASE IF NOT EXISTS dbcandidates")
cursor.execute("USE dbcandidates")

create_table_query = """
    CREATE TABLE IF NOT EXISTS candidates (
        candidate_id INT AUTO_INCREMENT PRIMARY KEY,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        email VARCHAR(200),
        application_date DATE,
        country VARCHAR(100),
        yoe INT,
        seniority VARCHAR(100),
        technology VARCHAR(200),
        code_challenge_score INT,
        technical_interview_score INT
    )
"""
cursor.execute(create_table_query)


# Read the CSV file
df = pd.read_csv("../data/candidates.csv", delimiter=";")

column = [column.replace(' ', '_') for column in df.columns] 


# Insertion query
query_insert = "INSERT INTO candidates ({}) VALUES ({})".format(
    ', '.join(column),
    ', '.join(['%s'] * len(column))
)

values_row = [tuple(row) for row in df.values]

# Insert the data into the table
cursor.executemany(query_insert, values_row)

connection.commit()
connection.close()
