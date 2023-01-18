import re
import cx_Oracle
import configparser
from datetime import datetime
import boto3
from botocore.config import Config
import sys
import os
import zipfile
import time

stage_db = 'stage'


def truncate_staging_table(conn, table_name):
    try:
        cursor = conn.cursor()
        query = "TRUNCATE TABLE " + stage_db + "." + table_name
        cursor.execute(query)
        conn.commit()
    except cx_Oracle.Error as e:
        print(e)
        conn.rollback()
        raise
    finally:
        cursor.close()


def get_db_token():
    client = boto3.client("rds", "us-east-1")
    TOKEN = client.generate_db_auth_token( < host_name_here >, < port_no_here >, < user_name_here >, < s3_region_here >)
    return TOKEN


def get_connection():
    try:
        conn =cx_Oracle.connect("hr/hr@orclpdb")
                                                                                                                           database = < db_name_here >)
        return conn
    except cx_Oracle.Error as e:
        print(e)
        raise