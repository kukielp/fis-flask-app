from flask import Flask
import boto3
from botocore.exceptions import ClientError
import json
import psycopg2

app = Flask(__name__)

def get_secret():
    secret_name = "flasksecret"
    region_name = "ap-southeast-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_name + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)
        elif e.response['Error']['Code'] == 'DecryptionFailure':
            print("The requested secret can't be decrypted using the provided KMS key:", e)
        elif e.response['Error']['Code'] == 'InternalServiceError':
            print("An error occurred on service side:", e)
    else:
        # Secrets Manager decrypts the secret value using the associated KMS CMK
        # Depending on whether the secret was a string or binary, only one of these fields will be populated
        if 'SecretString' in get_secret_value_response:
            text_secret_data = get_secret_value_response['SecretString']
        else:
            text_secret_data = get_secret_value_response['SecretBinary']
        return text_secret_data

def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE vendors (
            vendor_id SERIAL PRIMARY KEY,
            vendor_name VARCHAR(255) NOT NULL
        )
        """,
        """ CREATE TABLE parts (
                part_id SERIAL PRIMARY KEY,
                part_name VARCHAR(255) NOT NULL
                )
        """,
        """
        CREATE TABLE part_drawings (
                part_id INTEGER PRIMARY KEY,
                file_extension VARCHAR(5) NOT NULL,
                drawing_data BYTEA NOT NULL,
                FOREIGN KEY (part_id)
                REFERENCES parts (part_id)
                ON UPDATE CASCADE ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE vendor_parts (
                vendor_id INTEGER NOT NULL,
                part_id INTEGER NOT NULL,
                PRIMARY KEY (vendor_id , part_id),
                FOREIGN KEY (vendor_id)
                    REFERENCES vendors (vendor_id)
                    ON UPDATE CASCADE ON DELETE CASCADE,
                FOREIGN KEY (part_id)
                    REFERENCES parts (part_id)
                    ON UPDATE CASCADE ON DELETE CASCADE
        )
        """)
    conn = None
    try:
        # read the connection parameters
        # connect to the PostgreSQL server
        creds = json.loads(get_secret())
        conn = psycopg2.connect(
            host=creds['host'],
            database="postgres",
            user=creds['username'],
            password=creds['password']
        )
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
def insert_vendor_list(vendor_list):
    """ insert multiple vendors into the vendors table  """
    sql = "INSERT INTO vendors(vendor_name) VALUES(%s)"
    conn = None
    try:
        # read database configuration
        creds = json.loads(get_secret())
        conn = psycopg2.connect(
            host=creds['host'],
            database="postgres",
            user=creds['username'],
            password=creds['password']
        )
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.executemany(sql,vendor_list)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
def get_vendors():
    """ query data from the vendors table """
    conn = None
    res = None
    try:
        creds = json.loads(get_secret())
        conn = psycopg2.connect(
            host=creds['host'],
            database="postgres",
            user=creds['username'],
            password=creds['password']
        )
        cur = conn.cursor()
        cur.execute("SELECT vendor_id, vendor_name FROM vendors ORDER BY vendor_name")
        print("The number of parts: ", cur.rowcount)
        res = cur.fetchall()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return res

@app.route('/')
def hello_world():
  return 'Hello world from Flask! The secret is: ' + get_secret()

@app.route('/select')
def select():
  return json.dumps(get_vendors())

@app.route('/load')
def load():
  create_tables()
  insert_vendor_list([
    ('AKM Semiconductor Inc.',),
    ('Asahi Glass Co Ltd.',),
    ('Daikin Industries Ltd.',),
    ('Dynacast International Inc.',),
    ('Foster Electric Co. Ltd.',),
    ('Murata Manufacturing Co. Ltd.',)
  ])
  return 'Data is loaded'
if __name__ == '__main__':
  app.run()