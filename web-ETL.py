import pandas as pd
from datetime import datetime
import psycopg2
import io
import config as cfg
from bs4 import BeautifulSoup
import requests
import sys

#### GLOBAL VARIABLES ####
log_file = "log_file.log"
table_name = "table"
data_url = 'url'

def extract():

    try:
        extracted_data = pd.DataFrame(columns=["column1","column2","column3",'column4'])

        response = requests.get(data_url)
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')

        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) > 0:
                    column1 = cells[0].text
                    column2 = cells[2].text
                    column3 = cells[3].text
                    column4 = cells[4].text

                    extracted_data = pd.concat([extracted_data, pd.DataFrame([{"column1":column1,"column2":column2, "column3":column3,'column4':column4}])], ignore_index = True)

        extracted_data.drop_duplicates(inplace=True)   

        return extracted_data

    except Exception as e:

        log_process(f"Error extracting data: {e}")
        sys.exit(1)
    


def transform(data):

    try:

        pass

    except Exception as e:
        log_process(f"Error transforming data: {e}")
        sys.exit(1)

    return data

def load_data(transformed_data,cur,conn):

    try:

        csv_buffer = io.StringIO()
        transformed_data.to_csv(csv_buffer, index=False, header=False) 
        csv_buffer.seek(0)

        cur.copy_expert(f"""COPY {table_name} ("column1","column2","column3") FROM STDIN WITH (FORMAT CSV)""", csv_buffer)
        conn.commit()

    except Exception as e:
            
        log_process(f"Error loading data: {e}")
        conn.rollback()
        cur.close()
        conn.close()
        sys.exit(1)
    
def log_process(msg):

    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now().strftime(timestamp_format)
    line = '[' + now + ']: ' + msg + '\n'

    print(line)
    with open(log_file, "a") as f:

        f.write(line)


if __name__ == "__main__":

    conn = psycopg2.connect(
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
        host=cfg.host,
        port=cfg.port 
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
    load_data(transformed_data,cur,conn) 
    log_process("Load phase Ended")

    log_process("ETL Job Ended")

    cur.close()
    conn.close()