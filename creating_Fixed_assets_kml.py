import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from collections import defaultdict
import json

import mysql.connector
import csv


def add_asset_name(file,prefix_flag):
    df=pd.read_csv(file)
    sql="SELECT asset_name FROM seekright_v3_poc.tbl_asset where asset_id=%s;"
    conx=mysql.connector.connect(host='Takeleap.in',user='seekright',password='Takeleap@123',port='3307')
    cur=conx.cursor()
    names=[]
    names_without=[]
    for i in range(len(df["asset_id"])):
        
        
        cur.execute(sql,(str(df['asset_id'][i]),))
        result=cur.fetchone()
        if prefix_flag==True:
            if df['lhs_rhs'][i]==1:
                prefix="LEFT"
                names.append(f"{prefix}_{result[0]}")
            elif df['lhs_rhs'][i]==2:
                prefix="RIGHT"
                print(i)
                # print(result[0])
                names.append(f"{prefix}_{result[0]}")
            
        else:
            names.append(f"{result[0]}")
        names_without.append(result[0])
    if prefix_flag:    
        df['asset_name']=names
    print(names)
    df['asset_name_without_prefix']=names_without
    # string_to_add = 'https://tlviz.s3.ap-south-1.amazonaws.com/SeekRight/'
    string_to_add="https://seekright.takeleap.in/SeekRight/"
    # string_to_add="https://images.seekright.ai/"
    try:
        df['image_path'] = (string_to_add + df['image_path']).str.replace(" ", "%20", regex=False)
    except:
        df['test_image_path'] = (string_to_add + df['test_image_path']).str.replace(" ", "%20", regex=False)

    
    df.to_csv(file.replace(".csv","_.csv"),index=False)

if __name__ == "__main__":

    db_file="/home/saran/Documents/Db_data/Invision_V_site2/chainage_correction/lhs_.csv"  # fixed assets csv sheet from db

    save_file_path=db_file

    file_name=save_file_path.split("/")[-1]
    add_asset_name(save_file_path,True)

    root_folder=save_file_path.replace(f"/{file_name}","")
    json_name=save_file_path.split("/")[-1].replace(".csv","")
    df = pd.read_csv(save_file_path.replace(".csv","_.csv"))

    assets = df['asset_name_without_prefix'].unique().tolist()

    df['coord'] = df.apply(lambda row: [row.latitude , row.longitude], axis=1)
    data = []
    count = 0
    from datetime import datetime
    for index, row in df.iterrows():  
        temp = dict.fromkeys(['row_id','asset_name', 'video_name', 'image_path', 'coord','deleted'])
        temp['row_id'] = row['row_id']
        temp['asset_name'] = row['asset_name']
        date=row['created_on']
        # timestamp = "2024-11-12 11:17:03"
        # formatted = datetime.strptime(date, "%Y-%m-%d %H:%M:%S").strftime("%Y_%m%d_%H%M%S_F")
        try:
            temp['image_path'] = row['image_path']
        except:
            temp['image_path'] = row['test_image_path']
        temp['coord'] = row['coord']
        temp['video_name']=row['video_name']
        if row['deleted']==1:
            temp['deleted']=True
        elif row['deleted']==0:
            temp['deleted']=False
        data.append(temp)
        count+=1
            
        if count % 10000000 == 0:
            with open(f"{root_folder}/{json_name}"+'_'+'_'+str(count)+".json", "w") as final:
                json.dump(data, final)
                data = []
        

    with open(f"{root_folder}/{json_name}"+'_'+'_'+str(count)+".json", "w") as final:
        json.dump(data, final)