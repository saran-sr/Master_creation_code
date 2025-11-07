import mysql.connector
import glob
import ast
import difflib
import os
import json
import shutil
from collections import defaultdict
from Insert.fixed_master import load_config
class AssetNameValidator:
    def __init__(self, db_config):
        self.db_config = db_config
        self.known_asset_names = []
        self.unknown_assets = defaultdict(list)  # {unknown_name: [list of json files where it appears]}
        self.matches_dict = {}
        self.approved_replacements = {}
        self.assets_to_add_to_db = []
        
    def connect_to_database(self):
        """Connect to database and fetch known asset names."""
        try:
            asset_mydb = mysql.connector.connect(**self.db_config)
            mycursor = asset_mydb.cursor(dictionary=True)
            config=load_config()
            server=config['server']
            if server =="anton":
                assets_db_name="seekright_v3_poc"
            elif server=="production":
                assets_db_name="seekright_v3"
            elif server=="enigma":
                assets_db_name="seekright_v3_enigma"
            else:
                print("Invalid server name")
            sql = f"SELECT asset_id, asset_name, asset_type, asset_synonyms FROM {assets_db_name}.tbl_asset;"
            mycursor.execute(sql)
            myresult = mycursor.fetchall()
            self.known_asset_names = [j['asset_name'] for j in myresult]
            asset_mydb.close()
            print(f"✅ Loaded {len(self.known_asset_names)} known asset names from database")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
        return True
    
    def clean_asset_name(self, raw_name):
        """Clean asset name according to the specified rules."""
        asset_name = raw_name.replace("\n", "")
        
        if "Bad_Lane" not in asset_name:
            asset_name = asset_name.replace("LEFT_", "").replace("RIGHT_", "").replace("_Start", "").replace("_End", "").replace("Bad_","").replace("_START","").replace("_END","").strip()
        else:
            asset_name = asset_name.replace("LEFT_", "").replace("RIGHT_", "").replace("_Start", "").replace("_End", "").replace("_START","").replace("_END","").strip()
        
        return asset_name
    
    def scan_json_files(self, folder):
        """Scan all JSON files in folder and identify unknown asset names."""
        print(f"🔍 Scanning JSON files in: {folder}")
        
        json_files = []
        for root, dirs, files in os.walk(folder):
            for file in files:
                if file.endswith('.json'):
                    json_files.append(os.path.join(root, file))
        
        print(f"Found {len(json_files)} JSON files to process")
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = ast.literal_eval(f.read())
            except Exception as e:
                print(f"⚠️ Failed to load {json_file}: {e}")
                continue
            
            for asset in data.get('Assets', []):
                raw_name = asset[0]
                asset_name = self.clean_asset_name(raw_name)
                
                if asset_name not in self.known_asset_names:
                    self.unknown_assets[asset_name].append(json_file)
        
        print(f"Found {len(self.unknown_assets)} unique unknown asset names")
    
    def find_closest_matches(self):
        """Find closest matches for unknown asset names with lenient matching."""
        print("🔗 Finding closest matches for unknown assets...")
        
        # Create lowercase versions for case-insensitive matching
        known_lower = [name.lower() for name in self.known_asset_names]
        
        # Directional synonyms and specific replacements
        directional_synonyms = {
            'directional': 'direction',
            'direction': 'directional',
            'traffic_signal_acos': 'Traffic_Signal_Junction',

        }
        
        for unknown_name in self.unknown_assets.keys():
            best_match = None
            
            # Try exact case-insensitive match first
            unknown_lower = unknown_name.lower()
            if unknown_lower in known_lower:
                idx = known_lower.index(unknown_lower)
                best_match = self.known_asset_names[idx]
            else:
                # Try directional synonyms
                for synonym, replacement in directional_synonyms.items():
                    if unknown_lower == synonym.lower():
                        test_name = replacement.lower()
                        if test_name in known_lower:
                            idx = known_lower.index(test_name)
                            best_match = self.known_asset_names[idx]
                            break
                
                # If no exact match, use difflib with lower cutoff for more lenient matching
                if not best_match:
                    # Try case-insensitive fuzzy matching
                    close = difflib.get_close_matches(unknown_lower, known_lower, n=1, cutoff=0.4)
                    if close:
                        idx = known_lower.index(close[0])
                        best_match = self.known_asset_names[idx]
            
            self.matches_dict[unknown_name] = best_match
        
        # Separate matched and unmatched
        matched = {k: v for k, v in self.matches_dict.items() if v is not None}
        unmatched = {k: v for k, v in self.matches_dict.items() if v is None}
        
        print(f"✅ Found close matches for {len(matched)} assets")
        print(f"❌ No matches found for {len(unmatched)} assets")
    
    def get_user_approval(self):
        """Present matches to user and get approval for replacements."""
        print("\n" + "="*80)
        print("ASSET NAME VALIDATION - USER APPROVAL REQUIRED")
        print("="*80)
        
        if not self.matches_dict:
            print("No unknown assets found that need approval.")
            return
        
        # Show matched assets for approval
        matched_assets = {k: v for k, v in self.matches_dict.items() if v is not None}
        
        if matched_assets:
            print(f"\nFound {len(matched_assets)} unknown assets with suggested matches:")
            print("-" * 60)
            
            for i, (unknown, suggested) in enumerate(matched_assets.items(), 1):
                files_count = len(self.unknown_assets[unknown])
                print(f"\n{i}. Unknown asset: '{unknown}'")
                print(f"   Suggested match: '{suggested}'")
                print(f"   Found in {files_count} JSON file(s)")
                
                while True:
                    choice = input(f"   Approve replacement? (y/n/q to quit): ").lower().strip()
                    if choice in ['y', 'yes']:
                        self.approved_replacements[unknown] = suggested
                        print(f"   ✅ Approved: '{unknown}' → '{suggested}'")
                        break
                    elif choice in ['n', 'no']:
                        self.assets_to_add_to_db.append(unknown)
                        print(f"   ❌ Rejected: '{unknown}' will be added to 'needs DB addition' list")
                        break
                    elif choice in ['q', 'quit']:
                        print("   🛑 Quitting approval process...")
                        return
                    else:
                        print("   Invalid input. Please enter 'y', 'n', or 'q'")
        
        # Add unmatched assets to the "needs DB addition" list
        unmatched_assets = [k for k, v in self.matches_dict.items() if v is None]
        if unmatched_assets:
            print(f"\n{len(unmatched_assets)} assets have no close matches and will be added to 'needs DB addition' list:")
            for asset in unmatched_assets:
                print(f"  - {asset}")
                self.assets_to_add_to_db.append(asset)
        
        print(f"\nSummary:")
        print(f"✅ Approved replacements: {len(self.approved_replacements)}")
        print(f"📝 Assets to add to DB: {len(self.assets_to_add_to_db)}")
    
    def perform_replacements(self, create_backup=True):
        """Replace approved asset names in JSON files."""
        if not self.approved_replacements:
            print("No approved replacements to perform.")
            return
        
        print(f"\n🔄 Performing {len(self.approved_replacements)} approved replacements...")
        
        files_to_process = set()
        for unknown_name in self.approved_replacements.keys():
            files_to_process.update(self.unknown_assets[unknown_name])
        
        print(f"Will process {len(files_to_process)} JSON files")
        
        if create_backup:
            print("📦 Creating backup of JSON files...")
        
        replaced_count = 0
        for json_file in files_to_process:
            if create_backup:
                backup_file = json_file + '.backup'
                shutil.copy2(json_file, backup_file)
            
            try:
                with open(json_file, 'r') as f:
                    content = f.read()
                    data = ast.literal_eval(content)
                
                modified = False
                for i, asset in enumerate(data.get('Assets', [])):
                    raw_name = asset[0]
                    print("raw_name",raw_name)
                    bad_flag=False
                    if "bad_" in raw_name.lower() and "Bad_Lane" not in raw_name:
                        bad_flag=True
                    cleaned_name = self.clean_asset_name(raw_name)
                    print("cleaned_name",cleaned_name)
                    if cleaned_name in self.approved_replacements:
                        # Reconstruct the original name with the replacement
                        new_name = raw_name

                        for old_name, new_name_clean in self.approved_replacements.items():
                            if cleaned_name == old_name:
                                # Replace the cleaned part while preserving prefixes/suffixes
                                if raw_name != "Bad_Lane":
                                    # Standard replacement

                                    new_name = raw_name.replace(old_name, new_name_clean)
                                    print("new_name",new_name)
                                else:
                                    # Special case for Bad_Lane
                                    # if bad_flag==True:
                                        # new_name_clean="Bad_"+new_name_clean
                                    # new_name = raw_name.replace(old_name, new_name_clean)
                                    print("new_name",new_name)
                                break
                        
                        data['Assets'][i][0] = new_name
                        modified = True
                        replaced_count += 1
                
                if modified:
                    with open(json_file, 'w') as f:
                        f.write(str(data))
                    print(f"  ✅ Updated: {os.path.basename(json_file)}")
                
            except Exception as e:
                print(f"  ❌ Error processing {json_file}: {e}")
        
        print(f"\n✅ Replacement complete! Updated {replaced_count} asset references")
    
    def print_summary(self):
        """Print final summary of the validation process."""
        print("\n" + "="*80)
        print("ASSET VALIDATION SUMMARY")
        print("="*80)
        
        if self.approved_replacements:
            print(f"\n✅ APPROVED REPLACEMENTS ({len(self.approved_replacements)}):")
            for old_name, new_name in self.approved_replacements.items():
                files_count = len(self.unknown_assets[old_name])
                print(f"  '{old_name}' → '{new_name}' (in {files_count} files)")
        
        if self.assets_to_add_to_db:
            print(f"\n📝 ASSETS THAT NEED TO BE ADDED TO DATABASE ({len(self.assets_to_add_to_db)}):")
            for asset in self.assets_to_add_to_db:
                files_count = len(self.unknown_assets[asset])
                print(f"  - '{asset}' (found in {files_count} files)")
        
        print("\n" + "="*80)

def print_master_anomaly(folder,anomalies):
    master=[]
    anomalies_list=[]
    print(f"🔍 Scanning JSON files in: {folder}")
    
    json_files = []
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))
    
    print(f"Found {len(json_files)} JSON files to process")
    
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                data = ast.literal_eval(f.read())
        except Exception as e:
            print(f"⚠️ Failed to load {json_file}: {e}")
            continue
        
        for asset in data.get('Assets', []):
            raw_name = asset[0]
            asset_name = raw_name.replace("\n", "")

        
            if "Bad_Lane" not in asset_name:
                asset_name = asset_name.replace("LEFT_", "").replace("RIGHT_", "").replace("_Start", "").replace("_End", "").replace("Bad_","").replace("_START","").replace("_END","").strip()
            else:
                asset_name = asset_name.replace("LEFT_", "").replace("RIGHT_", "").replace("_Start", "").replace("_End", "").strip()
            if asset_name not in anomalies and asset_name not in master:
                master.append(asset_name)
            elif asset_name in anomalies and asset_name not in anomalies_list:
                anomalies_list.append(asset_name)
            else:
                pass
    
    return master, anomalies_list

def validate_and_modify_json(folder):
    # Database configuration
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
    config_file = os.path.normpath(config_path) 

    with open(config_file) as file:
        config = json.load(file)

    server=config['server']
    anomalies=config['anomalies']

    if server=="anton" or server=="production" or server=="enigma":
        print(f"Connecting to {server} database...")
    if server=="anton" or server=="production" or server=="enigma":   
        db_config = {
            "anton": {
                "host": "takeleap.in",
                "user": "seekright",
                "password": "Takeleap@123",
                "port": 3307
            },
            "production": {
                "host": "seekright-db.ce3lsmnwzkln.ap-south-1.rds.amazonaws.com",
                "user": "admin",
                "password": "BXWUCSpjRxEqzxXYTF9e",
                "port": 3306
            },
            "enigma": {
                "host": "mariadb.seekright.ai",
                "user": "enigma",
                "password": "Takeleap@123",
                "port": 3307
            }
        }

    else:
        print("Invalid server name in config file. Please check the config.json file.")
        exit(1)
    
    # Folder containing JSON files
    db_config = db_config.get(server)
    
    # Initialize validator
    validator = AssetNameValidator(db_config)
    
    # Step 1: Connect to database and get known asset names
    if not validator.connect_to_database():
        print("Cannot proceed without database connection.")
        return
    
    # Step 2: Scan JSON files for unknown asset names
    validator.scan_json_files(folder)
    
    if not validator.unknown_assets:
        print("🎉 All asset names in JSON files are already known in the database!")
        
    
    # Step 3: Find closest matches
    validator.find_closest_matches()
    
    # Step 4: Get user approval for replacements
    validator.get_user_approval()
    
    # Step 5: Perform approved replacements
    if validator.approved_replacements:
        confirm = input(f"\nProceed with replacing {len(validator.approved_replacements)} asset names? (y/n): ").lower().strip()
        if confirm in ['y', 'yes']:
            validator.perform_replacements(create_backup=True)
        else:
            print("Replacement cancelled by user.")
    
    # Step 6: Print final summary
    validator.print_summary()
    print("Please add the anomalies to the anomalies list in the config.json file.")
    m,a=print_master_anomaly(folder,anomalies)
    print("******************************")
    print("MASTER assets")
    print("******************************")
    for val in m:
        print(val)
    print("******************************")
    print("ANOMALY assets")
    print("******************************")
    for val2 in a:
        print(val2)

    user_input_2=str(input("Is the list correct ")).lower().strip()
    if "y" == user_input_2:
        pass
    else:
        print("Exiting...")
        print("Please add the anomalies to the anomalies list in the config.json file.")
        exit(0)


if __name__ == "__main__":
    validate_and_modify_json()