import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import psycopg2
import io

#### GLOBAL VARIABLES ####
log_file = "log_file.log"
table_name = "table"


def extract_from_csv(file_2_process):

    return pd.read_csv(file_2_process)

def extract_from_json(file_2_process):

    return pd.read_json(file_2_process,lines=True)

def extract_from_xml(file_2_process):

    df = pd.DataFrame(columns=["column1","column2","column3"])

    tree = ET.parse(file_2_process)
    root = tree.getroot()

    for object in root:

        column1 = object.find("column1").text
        column2 = float(object.find("column2").text)
        column3 = float(object.find("column3").text)

        df = pd.concat([df, pd.DataFrame([{"column1":column1,"column2":column2, "column3":column3}])], ignore_index = True)
    
    return df

def extract():

    extracted_data = pd.DataFrame(columns=["column1","column2","column3"])

    ## Process all csv files
    for csv_file in glob.glob("*.csv"):

        extracted_data = pd.concat([extracted_data,extract_from_csv(csv_file)],ignore_index=True)
    
    ## Process all json files
    for json_file in glob.glob("*.json"):

        extracted_data = pd.concat([extracted_data,extract_from_json(json_file)],ignore_index=True)
    
    ## Process all xml files
    for xml_file in glob.glob("*.xml"):

        extracted_data = pd.concat([extracted_data,extract_from_xml(xml_file)],ignore_index=True)
    
    return extracted_data


def transform(data):

    ### TRANSFORMATION  HERE ###

    return data

def load_data(transformed_data,cur,conn):

    csv_buffer = io.StringIO()
    transformed_data.to_csv(csv_buffer, index=False, header=False)  # No escribir el encabezado

    csv_buffer.seek(0)

    cur.copy_from(csv_buffer, table_name, sep=',')
    conn.commit()

def log_process(msg):

    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now().strftime(timestamp_format)
    line = now + ',' + msg + '\n'

    print(line)
    with open(log_file, "a") as f:

        f.write(line)


if __name__ == "__main__":

    conn = psycopg2.connect(
        dbname="db_name",
        user="user",
        password="password",
        host="localhost"  
    )

    cur = conn.cursor()
    log_process("ETL Job Started")

    log_process("Extract phase Started")
    extracted_data = extract()
    log_process("Extract phase Ended")

    log_process("Transform phase Started")
    transformed_data = transform(extracted_data)
    log_process("Transformed data")

    log_process("Load phase Started")
    ### BULK LOAD EXAMPLE ###
    load_data(transformed_data,cur,conn) 
    log_process("Load phase Ended")

    log_process("ETL Job Ended")
