import json,os
import pandas as pd
from db.connect import get_database_connection
from Insert.fixed_master import load_config


def get_asset_name(df,server):
    if server =="anton":
        assets_db_name="seekright_v3_poc"
    elif server=="production":
        assets_db_name="seekright_v3"
    elif server=="enigma":
        assets_db_name="seekright_v3_poc"
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

def convert_to_json(df, root_folder, json_name):
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
            with open(f"{root_folder}/{json_name}"+'_'+str(count)+".json", "w") as final:
                json.dump(data, final)
                data = []
        

    with open(f"{root_folder}/{json_name}"+'_'+str(count)+".json", "w") as final:
        json.dump(data, final)

def get_asset_name(df,server):
    
    
    
    if server =="anton":
        string_to_add="https://seekright.takeleap.in/SeekRight/"
        assets_db_name="seekright_v3_poc"
        df['image_path']=string_to_add+df['image_path']
    elif server=="production":
        string_to_add = 'https://tlviz.s3.ap-south-1.amazonaws.com/SeekRight/'
        assets_db_name="seekright_v3"
        df['image_path']=string_to_add+df['image_path']
    elif server=="enigma":
        string_to_add="https://images.seekright.ai/"
        assets_db_name="seekright_v3_poc"
        df['image_path']=string_to_add+df['image_path']
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

def get_asset_with_large_count(df):
    id_=df['asset_id'].value_counts().idxmax()
    name=df[df['asset_id']==id_]['asset_name'].iloc[0]

    return id_,name

def create_file(df,server,kml_folder):
    df=get_asset_name(df,server)
    
    max_asset_id,max_asset_name=get_asset_with_large_count(df)
    print(max_asset_id,max_asset_name) #debugging
    root_folder=kml_folder
    temp_df1=df[df['asset_id']==max_asset_id]
    convert_to_json(temp_df1, root_folder,max_asset_name)
    temp_df2=df[df['asset_id']!=max_asset_id]
    convert_to_json(temp_df2, root_folder,f"_all_assets_except_{max_asset_name}")
    

def create_kml(first_id, last_id):
    config=load_config()
    server=config["server"]
    kml_input_folder=config['kml_input_folder_path']
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
    # try:
    #     cursor.execute(f"SELECT * FROM {db_name}.tbl_site_asset WHERE id BETWEEN {first_id} AND {last_id};")
    #     print(f"SELECT * FROM {db_name}.tbl_site_asset WHERE id BETWEEN {first_id} AND {last_id};")
    #     data = cursor.fetchall()
    #     print(data)
    #     df = pd.DataFrame(data)
    #     df.to_csv(db_file_save_location, index=False)
    #     create_file(df,server,kml_input_folder)
    #     print(f"Data saved to {kml_input_folder}")
    # except Exception as e:
    #     print(f"Error fetching data from database: {e}")
    #     return
    
    cursor.execute(f"SELECT * FROM {db_name}.tbl_site_asset WHERE id BETWEEN {first_id} AND {last_id};")
    print(f"SELECT * FROM {db_name}.tbl_site_asset WHERE id BETWEEN {first_id} AND {last_id};")

    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]  # fix: extract column names
    
    df = pd.DataFrame(data, columns=columns)  # apply column names
    df=get_asset_name(df,server)
    if config['kml_left_right_assets_separate']:
        left_street_light = df[(df['asset_id'] == 33) & (df['lhs_rhs'] == 1)]
        right_street_light = df[(df['asset_id'] == 33) & (df['lhs_rhs'] == 2)]

        left_other_assets = df[(df['asset_id'] != 33) & (df['lhs_rhs'] == 1)]
        right_other_assets = df[(df['asset_id'] != 33) & (df['lhs_rhs'] == 2)]
        left_street_light.to_csv(kml_input_folder+"/left_street_light.csv", index=False)
        right_street_light.to_csv(kml_input_folder+"/right_street_light.csv",index=False)
        left_other_assets.to_csv(kml_input_folder+"/left_other_assets.csv",index=False)
        right_other_assets.to_csv(kml_input_folder+"/right_other_assets.csv",index=False)
        print("Left and Right assets saved separately.")
        convert_to_json(left_street_light, kml_input_folder, "left_street_light")
        convert_to_json(right_street_light, kml_input_folder, "right_street_light")
        convert_to_json(left_other_assets, kml_input_folder, "left_other_assets")
        convert_to_json(right_other_assets, kml_input_folder, "right_other_assets")
    #     create_file(left_street_light,server,kml_input_folder)
    #     create_file(right_street_light,server,kml_input_folder)
    #     create_file(left_other_assets,server,kml_input_folder)
    #     create_file(right_other_assets,server,kml_input_folder)      
        
    df.to_csv(db_file_save_location, index=False)
    # create_file(df, server, kml_input_folder)

    print(f"Data saved to {kml_input_folder}")    
    
    


 

