import pandas as pd
import glob
import mysql.connector
import time
from concurrent.futures import ThreadPoolExecutor
import re
import os
import datetime
import csv,json



def get_db_data(cursor, site_id, db_name):
    """Function to get database data and return as DataFrame."""
    
    # Build the query
    query = f"""SELECT * FROM {db_name}.tbl_site_asset;"""  

    try:
        cursor.execute(query)
        column_names = [column[0] for column in cursor.description]
        rows = cursor.fetchall()

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=column_names)

        print(f"DataFrame created successfully. Total rows: {len(df)}")
        print(f"Columns: {df.columns.tolist()}")
        return df

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return pd.DataFrame()  # return empty DataFrame on error
    
config_file="./config.json"

with open(config_file) as file:
    config = json.load(file)






server=config['server']
site_id=config['site_id']
db_name=config['db_name']
folder=config['kml_folder_path']

if server=="anton" or server=="production" or server=="enigma":
    print(f"Connecting to {server} database...")
if server=="anton":   
    conx=mysql.connector.connect(host='takeleap.in',user='seekright',password='Takeleap@123',port='3307')
elif server=="production":
    conx= mysql.connector.connect(host='seekright-db.ce3lsmnwzkln.ap-south-1.rds.amazonaws.com',user='admin',password='BXWUCSpjRxEqzxXYTF9e',port='3306')
elif server=="enigma":
    conx = mysql.connector.connect(host='mariadb.seekright.ai', user='enigma', password='Takeleap@123', port='3307')
else:
    print("Invalid server name in config file. Please check the config.json file.")
    exit(1)


cur=conx.cursor()

db_df=get_db_data(cur,site_id,db_name)

current_time_stamp=datetime.datetime.now()
row_id_to_id_dict = db_df.set_index('row_id')['id'].to_dict()
update_comment=f"Update Latlong on {current_time_stamp}"
delete_comment=f"Deleted on {current_time_stamp}"
def replace_row_id_with_id(row_id, mapping):
    """Replace row_id with id using the mapping dictionary."""
    return mapping.get(row_id, row_id)  # Default to row_id if no match found

update_query_id=f"Update {db_name}.tbl_site_asset set latitude=%s,longitude=%s ,Description=%s where id=%s" 
update_query_row_id=f"Update {db_name}.tbl_site_asset set latitude=%s,longitude=%s ,Description=%s where row_id=%s" 
delete_query_id=f"Update {db_name}.tbl_site_asset set deleted=1,Description=%s where id=%s"
delete_query_row_id=f"Update {db_name}.tbl_site_asset set deleted=1,Description=%s where row_id=%s"
# file="/home/saran/POC/Chile_POC/chile_Fixed_.csv"


for root, dirs, files in os.walk(folder):
    for file in files:
        file=os.path.join(root, file)
        print(file)
        df=pd.read_csv(file)
        if 'start_latlong' in df.columns:
            continue

        for i in range(len(df['row_id'])):

            if df['deleted'][i]=="No" and df['modified'][i]=="Yes":
                id_=replace_row_id_with_id(df['row_id'][i],row_id_to_id_dict)
                if id_ != df['row_id'][i]:
                    print("Updated latlong with id ",df['row_id'][i],id_)
                    cur.execute(update_query_id,(df['latitude'][i],df['longitude'][i],update_comment,id_))

                else:
                    print("Updated latlong with row_id ",df['row_id'][i])
                    cur.execute(update_query_row_id,(df['latitude'][i],df['longitude'][i],update_comment,df['row_id'][i]))
            elif df['deleted'][i]=="Yes":
                id_=replace_row_id_with_id(df['row_id'][i],row_id_to_id_dict)
                if id_ != df['row_id'][i]:
                    print("Deleted  with id ",df['row_id'][i],id_)
                    print(delete_comment,id_)
                    cur.execute(delete_query_id,(delete_comment,id_))

                else:
                    print("deleted with row_id ",df['row_id'][i])
                    print(delete_comment,df['row_id'][i])
                    cur.execute(delete_query_row_id,(delete_comment,df['row_id'][i]))

conx.commit()
conx.close()
    