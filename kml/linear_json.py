import os
import pandas as pd
from datetime import datetime
import json


from Insert.fixed_master import load_config
from db.connect import get_database_connection


def image_url(server):
    if server =="anton":
        string_to_add="https://seekright.takeleap.in/SeekRight/"
        return string_to_add
    elif server=="production":
        string_to_add = 'https://tlviz.s3.ap-south-1.amazonaws.com/SeekRight/'
        return string_to_add
    elif server=="enigma":
        string_to_add="https://images.seekright.ai/"
        return string_to_add
    else:
        print("Invalid server name")

def get_asset_name(df,server):

    if server =="anton":
        # string_to_add="https://seekright.takeleap.in/SeekRight/"
        assets_db_name="seekright_v3_poc"
        # df['image_path']=string_to_add+df['image_path']
    elif server=="production":
        # string_to_add = 'https://tlviz.s3.ap-south-1.amazonaws.com/SeekRight/'
        assets_db_name="seekright_v3"
        # df['image_path']=string_to_add+df['image_path']
    elif server=="enigma":
        # string_to_add="https://images.seekright.ai/"
        assets_db_name="seekright_v3_enigma"
        # df['image_path']=string_to_add+df['image_path']
    else:
        print("Invalid server name")

    sql=f"SELECT asset_name FROM {assets_db_name}.tbl_asset where asset_id=%s;"
    conx=get_database_connection(server)
    cur=conx.cursor()
    names=[]
    for i in range(len(df["asset_id"])):
        cur.execute(sql,(str(df['asset_id'][i]),))
        result=cur.fetchone()
        names.append(result[0])
    df['asset_name']=names
    
    
    return df


def linear_kml(first_L,last_L):
    config=load_config()
    server=config["server"]
    kml_input_folder=config['json_folder'].replace(config['json_folder'].split("/")[-1],"linear_kml")

    url= image_url(server)
    if not os.path.exists(kml_input_folder):
        os.makedirs(os.path.dirname(kml_input_folder),exist_ok=True)
    
    
    
    db_file_save_location = kml_input_folder+"/db_data/site_asset_data.csv"
    csv_file_path=kml_input_folder+"/site_asset_data.csv"
    os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
    os.makedirs(os.path.dirname(db_file_save_location), exist_ok=True)
    conx = get_database_connection(server)
    if conx is None:
        print("Failed to connect to the database.")
        return
    db_name = config["db_name"]
    cursor = conx.cursor(dictionary=True)
    
    cursor.execute(f"SELECT * FROM {db_name}.tbl_site_asset WHERE id BETWEEN {first_L} AND {last_L};")
    print(f"SELECT * FROM {db_name}.tbl_site_asset WHERE id BETWEEN {first_L} AND {last_L};")

    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]  # fix: extract column names
    
    df = pd.DataFrame(data, columns=columns)  # apply column names
    df=get_asset_name(df,server)
    assets = set(list(df["asset_name"]))
    df['coord'] = df.apply(lambda row: [row.latitude , row.longitude], axis=1)
    for asset in assets:
        # df_ = df[df['asset_name']== asset]
        df_ = df[df["asset_name"]== asset]
        print(asset)
        data = []
        count = 0
        
        for index, row in df_.iterrows():  
            temp = dict.fromkeys(['row_id','asset_name', 'video_name', 'image_path', 'coord'])
            temp['row_id'] = row['row_id']
            #temp['asset_name'] = row['asset_name']
            t = row['image_path'].split(",")
            if len(t)==3:
                i,j,k = t
            else:
                i,j,k = row['image_path'],row['image_path'],row['image_path']
 
            try:
                temp['video_name'] = row['video_name']
            except:
                date=row['created_on']
                formatted = datetime.strptime(date, "%Y-%m-%d %H:%M:%S").strftime("%Y_%m%d_%H%M%S_F")
                # print(formatted)
                temp['video_name'] =formatted
            temp['asset_name']=row["asset_name"]
            temp['image_path1'] = url+i
            temp['image_path2'] = url+j
            temp['image_path3'] = url+k

            
            temp['coord'] = row['coord']
            temp['coord_lat'] = row["latitude"]
            temp['coord_lng'] = row["longitude"]
            if 'deleted' in df.columns:
                temp["deleted"] = True if row["deleted"]==1 else False
            else:
                temp['deleted']=False
            
            if "None" not in str(row['start_latlong']) and 'nan' not in str(row['start_latlong']):
                if "N" in row['start_latlong'] and "E" in row['start_latlong']:
                    temp['start_coord_lat'] = row['start_latlong'].split("E")[0].replace("N","")
                    temp['start_coord_lng'] = row['start_latlong'].split("E")[1]
                    temp['end_coord_lat'] = row['end_latlong'].split("E")[0].replace("N","")
                    temp['end_coord_lng'] = row['end_latlong'].split("E")[1]
                elif "N" in row['start_latlong'] and "W" in row['start_latlong']:
                    temp['start_coord_lat'] = row['start_latlong'].split("W")[0].replace("N", "")
                    temp['start_coord_lng'] = "-"+row['start_latlong'].split("W")[1]
                    temp['end_coord_lat'] = row['end_latlong'].split("W")[0].replace("N", "")
                    temp['end_coord_lng'] = "-"+row['end_latlong'].split("W")[1]
                elif "S" in row['start_latlong'] and "E" in row['start_latlong']:
                    temp['start_coord_lat'] = "-"+row['start_latlong'].split("E")[0].replace("S", "")
                    temp['start_coord_lng'] = row['start_latlong'].split("E")[1]
                    temp['end_coord_lat'] = "-"+row['end_latlong'].split("E")[0].replace("S", "")
                    temp['end_coord_lng'] = row['end_latlong'].split("E")[1]
                elif "S" in row['start_latlong'] and "W" in row['start_latlong']:
                    temp['start_coord_lat'] = "-"+row['start_latlong'].split("W")[0].replace("S", "")
                    temp['start_coord_lng'] = "-"+row['start_latlong'].split("W")[1]
                    temp['end_coord_lat'] = "-"+row['end_latlong'].split("W")[0].replace("S", "")
                    temp['end_coord_lng'] = "-"+row['end_latlong'].split("W")[1]
                else:
                    pass
            else:
                # print("None",row["latitude"])

                temp['start_coord_lat'] = row["latitude"]
                temp['start_coord_lng'] = row["longitude"]
                temp['end_coord_lat'] = row["latitude"]
                temp['end_coord_lng'] = row["longitude"]

            data.append(temp)
            count+=1
            
            # if count % 100000 == 0:
            #     with open(str(section)+'_'+asset+'_'+str(count)+".json", "w") as final:
            #         json.dump(data, final)
            #         data = []
            

        with open(f'{kml_input_folder}/'+asset+'_'+str(count)+".json", "w") as final:
            print(f'{kml_input_folder}/'+asset+'_'+str(count)+".json")
            json.dump(data, final)