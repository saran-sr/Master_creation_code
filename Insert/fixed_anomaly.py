import mysql.connector
import pandas as pd
import os
import json
import ast
from datetime import datetime

def load_config():
	"""Load configuration from config.json file"""
	config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
	config_file = os.path.normpath(config_path)
	
	try:
		with open(config_file) as f:
			config = json.load(f)
		return config
	except FileNotFoundError:
		print(f"Config file not found: {config_file}")
		return None
	except json.JSONDecodeError as e:
		print(f"Invalid JSON in config file: {e}")
		return None

def get_database_connection(server):
	"""Establish database connection using the specific logic"""
	if server=="anton":   
		conx=mysql.connector.connect(host='takeleap.in',user='seekright',password='Takeleap@123',port='3307')
	elif server=="production":
		conx= mysql.connector.connect(host='seekright-db.ce3lsmnwzkln.ap-south-1.rds.amazonaws.com',user='admin',password='BXWUCSpjRxEqzxXYTF9e',port='3306')
	elif server=="enigma":
		conx = mysql.connector.connect(host='mariadb.seekright.ai', user='enigma', password='Takeleap@123', port='3306')
	
	return conx


def fetch_asset_data(cursor,server):
	"""Fetch asset data from database"""
	if server =="anton":
		assets_db_name="seekright_v3_poc"
	elif server=="production":
		assets_db_name="seekright_v3"
	elif server=="enigma":
		assets_db_name="seekright_v3_poc"
	else:
		print("Invalid server name")

	
	try:
		sql = f"SELECT asset_id,asset_name,asset_type,asset_synonyms FROM {assets_db_name}.tbl_asset;"
		cursor.execute(sql)
		myresult = cursor.fetchall()
		
		d = {'asset_id': [], 'asset_name': [], 'asset_type': [], 'asset_synonyms': []}
		for j in myresult:
			d['asset_id'].append(j['asset_id'])
			d['asset_name'].append(j['asset_name'])
			d['asset_type'].append(j['asset_type'])
			d['asset_synonyms'].append(j['asset_synonyms'])
		
		return d
	except Exception as e:
		print(f"Error fetching asset data: {e}")
		return None

def parse_video_name_to_datetime(video_name):
	"""Parse video name to datetime object using the specific logic"""
	try:
		year = video_name[:4]
		month = video_name[5:7]
		day = video_name[7:9]
		hour = video_name[10:12] 
		minute = video_name[12:14]
		second = video_name[14:16]
		date_time_str = f"{year}-{month}-{day} {hour}:{minute}:{second}"
		date_time_obj = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
		sql_date_time = date_time_obj.strftime('%Y-%m-%d %H:%M:%S')
		return sql_date_time, True
	except:
		return None, False

def parse_location_string(location_str):
	"""Parse location string to latitude and longitude using the specific logic"""
	try:
		minus_flag_lat = False
		minus_flag_lon = False
		
		if "S" in location_str and "E" in location_str:
			minus_flag_lat, minus_flag_lon = True, False
			print(location_str, "location_str")
			e_index = location_str.index('E')
		elif "S" in location_str and "W" in location_str:
			minus_flag_lat, minus_flag_lon = True, True
			print(location_str, "location_str")
			e_index = location_str.index('W')
		else:
			print(location_str, "location_str")
			e_index = location_str.index('E')
		
		latitude = float(location_str[1:e_index])
		longitude = float(location_str[e_index + 1:])
		
		if minus_flag_lat == True:
			latitude = "-" + str(latitude)
		if minus_flag_lon == True:
			longitude = "-" + str(longitude)
		
		return latitude, longitude
	except Exception as e:
		print(f"Error parsing location string {location_str}: {e}")
		return None, None

def get_asset_info(column, asset_data):
	"""Get asset ID and type from column name"""
	asset_name = column.split("_", 1)[1]  # Extract asset_name from column name
	print("asset name in get asset_info",asset_name) #debugging
	if asset_name in asset_data['asset_name']:
		index = asset_data['asset_name'].index(asset_name)
		asset_id = asset_data['asset_id'][index]
		asset_type = asset_data['asset_type'][index]
		return asset_id, asset_type
	elif asset_name == "Tunnel_Traffic_Barriers":
		return 233, None  # Special case
	else:
		print(f"Asset not found in database: {asset_name}")
		return None, None

def should_skip_asset(check,exclusion_list):
	"""Check if asset should be skipped based on exclusion list"""
	# exclusion_list = [
	#     "Earth_Works", "Encroachment", "Drainage", "Flex_Banner", "Median_Plants",
	#     "Patch", "Cracks", "Potholes", "Water_Stagnation", "Sand_Accumulation",
	#     "Garbage", "Bad_Lane", "Shop_Board", "Wall_Poster"
	# ]
	return check in exclusion_list

def upload_to_database_anomaly_F(master_file):
	"""Main function to upload data to database"""
	
	# Load configuration
	config = load_config()
	if not config:
		print("Failed to load configuration")
		return
	
	# Load master CSV file
	
	if not os.path.exists(master_file):
		print(f"Master file not found: {master_file}")
		return
	
	try:
		lhs_df = pd.read_csv(master_file)
		print(f"Loaded master file: {len(lhs_df)} rows")
	except Exception as e:
		print(f"Error loading master file: {e}")
		return
	server= config['server']
	
	# Remove unnecessary columns
	columns_to_drop = ['Position', 'Start_frame', 'End_frame', 'Speed', 'video_name', 'Distance', 'Chainage']
	lhs_df = lhs_df.drop(columns=[col for col in columns_to_drop if col in lhs_df.columns])
	
	# Database connection
	conx = get_database_connection(server)
	if not conx:
		return
	
	cursor = conx.cursor(dictionary=True)
	
	# Fetch asset data
	asset_data = fetch_asset_data(cursor,server)
	if not asset_data:
		conx.close()
		return
	
	# Configuration values - using your specific logic
	site_id = config['site_id']
	lane_category_i = 1 if config.get('service_road_flag', False) else 3
	dir_name = config['image_directory']
	if dir_name[-1] !="/":
		dir_name=dir_name+"/"
	db_name=config['db_name']
	skip_assets=config['anomalies']
	# Process each column
	total_records = 0
	successful_inserts = 0
	# if get_id_from_db(server,db_name) is not None:
	# 	first_id_in_db = get_id_from_db(server,db_name) + 1
	# else:
	# 	first_id_in_db = 1

	for column in lhs_df.columns:
		print("column", column)
		check = str(column)
		check = check.replace("LEFT_", "")
		check = check.replace("RIGHT_", "")
		print("check", check)
		
		# if not should_skip_asset(check,skip_assets) or tup[-3] !="":
		# 	pass
		# else:
		# 	continue
			
		if not (column.startswith("RIGHT_") or column.startswith("LEFT_")):
			continue
		
		# Get asset information
		asset_id, asset_type = get_asset_info(column, asset_data)
		if not asset_id:
			continue
		
		print(asset_id)
		print(asset_type)
		
		# Determine side
		if column.startswith("RIGHT_"):
			lhs_or_rhs = 2
		elif column.startswith("LEFT_"):
			lhs_or_rhs = 1
		
		# Process each row in the column
		for j in lhs_df[column]:
			if j == "[]" or not bool(j):
				continue
			
			if asset_type in (1, 2, 3, 4):
				try:
					# Parse the string representation of the list
					j_list = eval(j)
					
					for tup in j_list:
						if type(tup) is None:
							continue
						total_records += 1
						if not should_skip_asset(check,skip_assets) and tup[-3] =="":
							continue
						# Extract data from tuple
						master_id = tup[0]
						chainage = tup[3]
						location_str = tup[4]
						video_name = tup[6]
						frame= tup[5]
						bbox = str(tup[-2])
						# bbox=str(bbox,frame)
						remark = tup[-3]
						comment=tup[-4]
						
						# Parse location
						latitude, longitude = parse_location_string(location_str)
						if latitude is None or longitude is None:
							continue
						
						print(video_name)
						print("After", master_id)
						
						# Create image path
						image_path = dir_name + str(master_id) + ".jpeg"
						
						# Parse video name to datetime
						sql_date_time, add_datetime_ = parse_video_name_to_datetime(video_name)
						
						# Insert into database
						try:
							if server=="anton":
								# if chainage and add_datetime_:
								# 	print(master_id, asset_id, site_id, chainage, latitude, longitude, image_path, lhs_or_rhs, asset_type, sql_date_time, lane_category_i, video_name)
								# 	values=(master_id, asset_id, site_id, chainage, latitude, longitude, image_path, lhs_or_rhs, asset_type, sql_date_time, lane_category_i, video_name,bbox)
								# 	for val in values:
								# 		print(val, type(val))
								# 	sql = f"""INSERT INTO {db_name}.tbl_site_anomaly
								# 			(row_id, asset_id, site_id, Chainage, latitude, longitude, image_path, lhs_rhs, 
								# 			number_anomaly, recent_anomaly, current_status, asset_type, created_on, updated_on, 
								# 			deleted_on, deleted, recent_anomaly_count, remark, Equipment_id, Description, lane_category, video_name, BBox) 
								# 			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, '0', NULL, '1', %s, %s, NULL, NULL, '0', '0', %s, NULL, NULL, %s, %s, %s)"""
								# 	cursor.execute(sql, (master_id, asset_id, site_id, chainage, latitude, longitude, image_path, lhs_or_rhs, asset_type, sql_date_time, remark, lane_category_i, video_name, bbox))
								# 	successful_inserts += 1
									
								# elif chainage and add_datetime_ == False:
								# 	print(master_id, asset_id, site_id, chainage, latitude, longitude, image_path, lhs_or_rhs, asset_type, lane_category_i, video_name)
									
								# 	sql = f"""INSERT INTO {db_name}.tbl_site_anomaly
								# 			(row_id, asset_id, site_id, Chainage, latitude, longitude, image_path, lhs_rhs, 
								# 			number_anomaly, recent_anomaly, current_status, asset_type, created_on, updated_on, 
								# 			deleted_on, deleted, recent_anomaly_count, remark, Equipment_id, Description, lane_category, video_name, BBox) 
								# 			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, '0', NULL, '1', %s, NULL, NULL, NULL, '0', '0', %s, NULL, NULL, %s, %s, %s)"""
								# 	cursor.execute(sql, (master_id, asset_id, site_id, chainage, latitude, longitude, image_path, lhs_or_rhs, asset_type, remark, lane_category_i, video_name, bbox))
								# 	successful_inserts += 1   
								if chainage and add_datetime_:
									print(master_id,asset_id,site_id, chainage, latitude, longitude, image_path,lhs_or_rhs,asset_type,sql_date_time,comment,remark)
									print("remark",remark)
									print("comment",comment)
									sql = f"""INSERT INTO {db_name}.tbl_site_anomaly
													(master_id, asset_id, site_id, Chainage, latitude, longitude, test_image_path,master_image_path, lhs_rhs, current_status, created_on, updated_on, 
													deleted_on, deleted,comment,remark, Equipment_id, Description,video_date) 
													VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s, '1', %s, NULL, NULL, '0', %s,%s, NULL, NULL,%s)"""
									cursor.execute(sql, (master_id,asset_id,site_id, chainage, latitude, longitude, image_path,image_path,lhs_or_rhs,sql_date_time,comment,remark,sql_date_time))
									successful_inserts += 1
								elif chainage and add_datetime_ == False:
									print(master_id,asset_id,site_id, chainage, latitude, longitude, image_path,lhs_or_rhs,asset_type,comment,remark)
									print("remark",remark)
									print("comment",comment)
									sql = f"""INSERT INTO {db_name}.tbl_site_anomaly
													(master_id, asset_id, site_id, Chainage, latitude, longitude, test_image_path,master_image_path, lhs_rhs, current_status, created_on, updated_on, 
													deleted_on, deleted,comment,remark, Equipment_id, Description,video_date) 
													VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s, '1', NULL, NULL, NULL, '0', %s,%s, NULL, NULL,NULL)"""
									cursor.execute(sql, (master_id,asset_id,site_id, chainage, latitude, longitude, image_path,image_path, lhs_or_rhs,comment,remark))
									successful_inserts += 1                         
							else:
								if chainage and add_datetime_:
									print(master_id,asset_id,site_id, chainage, latitude, longitude, image_path,lhs_or_rhs,asset_type,sql_date_time,comment,remark)
									print("remark",remark)
									print("comment",comment)
									sql = f"""INSERT INTO {db_name}.tbl_site_anomaly
													(master_id, asset_id, site_id, Chainage, latitude, longitude, test_image_path,master_image_path, lhs_rhs, current_status, created_on, updated_on, 
													deleted_on, deleted,comment,remark, Equipment_id, Description,video_date) 
													VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s, '1', %s, NULL, NULL, '0', %s,%s, NULL, NULL,%s)"""
									cursor.execute(sql, (master_id,asset_id,site_id, chainage, latitude, longitude, image_path,image_path,lhs_or_rhs,sql_date_time,comment,remark,sql_date_time))
									successful_inserts += 1
								elif chainage and add_datetime_ == False:
									print(master_id,asset_id,site_id, chainage, latitude, longitude, image_path,lhs_or_rhs,asset_type,comment,remark)
									print("remark",remark)
									print("comment",comment)
									sql = f"""INSERT INTO {db_name}.tbl_site_anomaly
													(master_id, asset_id, site_id, Chainage, latitude, longitude, test_image_path,master_image_path, lhs_rhs, current_status, created_on, updated_on, 
													deleted_on, deleted,comment,remark, Equipment_id, Description,video_date) 
													VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s, '1', NULL, NULL, NULL, '0', %s,%s, NULL, NULL,NULL)"""
									cursor.execute(sql, (master_id,asset_id,site_id, chainage, latitude, longitude, image_path,image_path,lhs_or_rhs,comment,remark))
									successful_inserts += 1                                
							if successful_inserts % 100 == 0:
								print(f"Processed {successful_inserts} records...")
								
						except Exception as e:
							print(f"Error inserting record {master_id}: {e}")
							continue
							
				except Exception as e:
					print(f"Error processing row data: {e}")
					continue
	
	# Commit changes
	try:
		conx.commit()
		print(f"Successfully uploaded {successful_inserts} out of {total_records} records to database")
		# last_id_in_db =get_id_from_db(server,db_name)
		# return first_id_in_db, last_id_in_db
	except Exception as e:
		print(f"Error committing changes: {e}")
		conx.rollback()
		return None, None
	
	# Close connection
	conx.close()
	print("Done")

if __name__ == "__main__":
	upload_to_database_anomaly_F()