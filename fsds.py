import os
import zipfile
import mysql.connector
import pandas
import logging
import numpy
import time
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)

db_config = {
    "host": "localhost",
    "user": "root",
    "password": "Mypass123456",
    "database": "mysql"
}

cols_list = [
    'abstract', 'accepted', 'aciks', 'adsh', 'afs',
     'baph', 'bas1', 'bas2', 'changed', 'cik', 'cityba',
     'cityma', 'coreg', 'countryba', 'countryinc', 'countryma',
     'crdr', 'custom', 'datatype', 'ddate', 'detail', 'doc',
     'ein', 'filed', 'footnote', 'form', 'former', 'fp', 'fy', 'fye',
     'inpth', 'instance', 'iord', 'line', 'mas1', 'mas2', 'name', 'nciks',
     'negating', 'period', 'plabel', 'prevrpt', 'qtrs', 'report', 'rfile',
     'sic', 'stmt', 'stprba', 'stprinc', 'stprma', 'tag', 'tlabel', 'uom',
     'value', 'version', 'wksi', 'zipba', 'zipma'
]


def execute_sql(sql_qry, sql_vals):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        if sql_vals is None:
            cursor.execute(sql_qry)
        else:
            cursor.executemany(sql_qry, sql_vals)

        if sql_qry.strip().lower().startswith("insert"):
            connection.commit()
        else:
            return cursor.fetchall()
    except Exception as err:
        print("Error:", err)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Connection closed.")


def create_secdb():
    # GLOBAL SQL MODE IS SET AS '' SO TO AVOID ERRORS CAUSED BY THE 'tag doc' COLUMN BEING TOO LONG
    sql = "CREATE DATABASE secdb;\nSET @@global.sql_mode='';"
    execute_sql(sql, None)


def create_sub_table(tbl):
    sql = f"""
        USE secdb;
        CREATE TABLE {tbl} (
            adsh VARCHAR(40),
            cik VARCHAR(40),
            name VARCHAR(150),
            sic VARCHAR(40),
            countryba VARCHAR(40),
            stprba VARCHAR(40),
            cityba VARCHAR(40),
            zipba VARCHAR(40),
            bas1 VARCHAR(40),
            bas2 VARCHAR(40),
            baph VARCHAR(40),
            countryma VARCHAR(40),
            stprma VARCHAR(40),
            cityma VARCHAR(40),
            zipma VARCHAR(40),
            mas1 VARCHAR(40),
            mas2 VARCHAR(40),
            countryinc VARCHAR(40),
            stprinc VARCHAR(40),
            ein VARCHAR(40),
            former VARCHAR(150),
            changed VARCHAR(40),
            afs VARCHAR(40),
            wksi VARCHAR(40),
            fye VARCHAR(40),
            form VARCHAR(40),
            period VARCHAR(40),
            fy VARCHAR(40),
            fp VARCHAR(40),
            filed VARCHAR(40),
            accepted VARCHAR(40),
            prevrpt VARCHAR(40),
            detail VARCHAR(40),
            instance VARCHAR(40),
            nciks VARCHAR(40),
            aciks VARCHAR(120)
        );
    """
    execute_sql(sql, None)


def create_num_table(tbl):
    sql = f"""
        USE secdb;
        CREATE TABLE {tbl} (
            adsh VARCHAR(40),
            tag VARCHAR(256),
            version VARCHAR(40),
            ddate VARCHAR(40),
            qtrs VARCHAR(40),
            uom VARCHAR(40),
            coreg VARCHAR(256),
            value VARCHAR(40),
            footnote VARCHAR(512)
        );
    """
    execute_sql(sql, None)


def create_pre_table(tbl):
    sql = f"""
        USE secdb;
        CREATE TABLE {tbl} (
            adsh VARCHAR(40),
            report VARCHAR(40),
            line VARCHAR(40),
            stmt VARCHAR(40),
            inpth VARCHAR(40),
            rfile VARCHAR(40),
            tag VARCHAR(256),
            version VARCHAR(40),
            plabel VARCHAR(512),
            negating VARCHAR(40)
        );
    """
    execute_sql(sql, None)


def create_tag_table(tbl):
    sql = f"""
        USE secdb;
        CREATE TABLE {tbl} (
            tag VARCHAR(256),
            version VARCHAR(40),
            custom VARCHAR(40),
            abstract VARCHAR(40),
            datatype VARCHAR(40),
            iord VARCHAR(40),
            crdr VARCHAR(40),
            tlabel VARCHAR(512),
            doc VARCHAR(10000),
        );
    """
    execute_sql(sql, None)


def create_table_by_name(table_name):
    if table_name.startswith("sub"):
        create_sub_table(table_name)
    elif table_name.startswith("num"):
        create_num_table(table_name)
    elif table_name.startswith("pre"):
        create_pre_table(table_name)
    elif table_name.startswith("tag"):
        create_tag_table(table_name)
    else:
        raise ValueError("Invalid table name")


def create_zip_list():
    url = "https://www.sec.gov/dera/data/financial-statement-data-sets"
    response = requests.get(url)
    zip_links = []
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        for link in soup.find_all("a", href=True):
            if link["href"].endswith(".zip"):
                zip_links.append(link["href"])
        zip_links = [(f'https://www.sec.gov{zip_link}', zip_link.split('/')[-1]) for zip_link in zip_links]
        zip_links.sort()
    else:
        print("Failed to retrieve the page.")
    return zip_links


def insert_zip_data(url, zip_file, table_names):
    response = requests.get(url)
    try:
        with open(zip_file, "wb") as f:
            print(f'Retrieving: {url}')
            f.write(response.content)
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            print(f'Unzipping: {zip_file}')
            zip_ref.extractall("unzipped_files")

        batch_size = 500  # MODIFY AS NEEDED

        for table_name in table_names:
            time.sleep(5)
            try:
                print(f'Parsing & Inserting: {table_name}')
                tbl = table_name.split('_')[0]
                txt_filename = f"unzipped_files/{tbl}.txt"
                df = pandas.read_csv(txt_filename, sep="\t")  # ADD ", low_memory=False" IF DESIRED
                df.replace(numpy.nan, -1, inplace=True)
                df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                total_rows = len(df)
                num_batches = (total_rows + batch_size - 1) // batch_size
                for column in df.columns:
                    df[column] = df[column].astype(str).str.strip()
                    if column not in cols_list and column is not None:
                        df.drop(column, inplace=True)

                for batch_num in range(num_batches):
                    start_idx = batch_num * batch_size
                    end_idx = min((batch_num + 1) * batch_size, total_rows)
                    batch_data = df.iloc[start_idx:end_idx]

                    data_to_insert = [tuple(row) for row in batch_data.values]
                    columns = batch_data.columns
                    placeholders = ",".join(["%s"] * len(columns))
                    sql = f"INSERT INTO secdb.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"

                    execute_sql(sql, data_to_insert)
                    print(f"Inserting: {num_batches} batches")
            except Exception as e:
                print(e)
    except Exception as e:
        print(e)


def create_and_insert_zip_data():
    tables = ["num", "pre", "sub", "tag"]
    zip_list = create_zip_list()
    if zip_list:
        for url, zip_file in zip_list:
            table_names = [f"{t}_{zip_file.split('.')[0]}" for t in tables]
            for table_name in table_names:
                print(f'Creating Table: {table_name}')
                create_table_by_name(table_name)
                time.sleep(0.5)
            insert_zip_data(url, zip_file, table_names)
    for z in zip_list:
        os.remove(z[-1])
    unzipdir = os.path.join(os.getcwd(), 'unzipped_files')
    for o in os.listdir(unzipdir):
        os.remove(f'{os.path.join(unzipdir, o)}')
    os.rmdir("unzipped_files")


def main():
    create_secdb()
    create_and_insert_zip_data()


if __name__ == "__main__":
    main()
