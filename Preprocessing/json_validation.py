import mysql.connector
import glob, ast, difflib



# Step 1: Get asset names from DB

asset_mydb = mysql.connector.connect(
    host='seekright-db.ce3lsmnwzkln.ap-south-1.rds.amazonaws.com',
    user='admin',
    password='BXWUCSpjRxEqzxXYTF9e',
    port='3306'
)

asset_mydb = mysql.connector.connect(
    host='takeleap.in',
    user='seekright',
    password='Takeleap@123',
    port='3307'
)

mycursor = asset_mydb.cursor(dictionary=True)
sql = "SELECT asset_id, asset_name, asset_type, asset_synonyms FROM seekright_v3_poc.tbl_asset;"
mycursor.execute(sql)
myresult = mycursor.fetchall()

known_asset_names = [j['asset_name'] for j in myresult]

# Step 2: Scan JSONs for unknown asset names
unknown_names_set = set()

for root,dirs,files in os.walk(folder):
    try:
        with open(json_file, 'r') as f:
            data = ast.literal_eval(f.read())
    except Exception as e:
        print(f"⚠️ Failed to load {json_file}: {e}")
        continue

    for asset in data.get('Assets', []):
        raw_name = asset[0].replace("\n", "")
        if asset_name !="Bad_Lane":
            asset_name = raw_name.replace("LEFT_", "").replace("RIGHT_", "").replace("_Start","").replace("_End","").strip()
        else:
            asset_name = raw_name.replace("LEFT_", "").replace("RIGHT_", "").replace("_Start","").replace("_End","").replace("Bad_","").strip()
        if asset_name not in known_asset_names:
            unknown_names_set.add(asset_name)

# Step 3: Match unknown names with closest known name
matches_dict = {}
for unknown_name in unknown_names_set:
    close = difflib.get_close_matches(unknown_name, known_asset_names, n=1, cutoff=0.6)
    matches_dict[unknown_name] = close[0] if close else None

# Step 4: Print or use the result
print(matches_dict)

if __name__=="__main__":
    
    # args=argument.Parser()
    folder = "/home/saran/Projects/adani/KKRPL_NEw/Fixed/LHS/SR/json"
