import os,shutil,ast,json

def get_names_from_folder(fixed_json_folder,linear_json_folder):
    files_match={}
    for root, dirs, files in os.walk(fixed_json_folder):
        for file in files:
            if file.endswith(".json"):
                key = file.replace("_final.json","")
                files_match[key] = [os.path.join(root, file)]
                print(f"Found fixed JSON file: {file} at {files_match[key]}")
    print(files_match)
    for root, dirs, files in os.walk(linear_json_folder):

        for file in files:
            if file.endswith(".json"):
                key = file.replace("_final.json","")
                if key in files_match:
                    files_match[key].append(os.path.join(root, file))
                    print(f"Found linear JSON file: {file} at {files_match[key][1]}")
    print(files_match)
    return files_match

def combine(fixed_file, linear_file, key,output_folder):
    combined_file_path = f"{output_folder}/{key}/{key}_final.json"

    os.makedirs(os.path.dirname(combined_file_path), exist_ok=True)
    combined_data = {'Assets': []}
    fixed_file_content = open(fixed_file, "r").read()
    fixed_data = ast.literal_eval(fixed_file_content)
    linear_file_content = open(linear_file, "r").read()
    linear_data = ast.literal_eval(linear_file_content)
    for i in range(len(fixed_data['Assets'])):
        print(fixed_data['Assets'][i])
        combined_data['Assets'].append(fixed_data['Assets'][i])
    for j in range(len(linear_data['Assets'])):
        print(linear_data['Assets'][j])
        if "start" in linear_data['Assets'][j][0].lower() or "end" in linear_data['Assets'][j][0].lower():
            print("Skipping asset:", linear_data['Assets'][j])
            continue
        combined_data['Assets'].append(linear_data['Assets'][j])
    # seen = set()
    # new_list = []
    # for d in combined_data['Assets']:
        
    #     if d not in seen:
    #         seen.add(d)
    #         new_list.append(d)
    
    # combined_data['Assets'] = new_list
    print(type(combined_data['Assets']))
    #remove duplicate from list
    print(combined_data['Assets'])
    seen = set()
    unique_assets = []
    for asset in combined_data['Assets']:
        asset_str = str(asset)
        if asset_str not in seen:
            seen.add(asset_str)
            unique_assets.append(asset)
    combined_data['Assets'] = unique_assets
    # exit()
    with open(combined_file_path, "w") as combined_file:
        combined_file.write(str(combined_data))
    
def combine_json_files():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
    config_file = os.path.normpath(config_path)
    with open(config_file) as f:
        config = json.load(f)

    fixed_json_folder=config['json_folder']
    linear_json_folder=config['linear_folder']
    output_folder=fixed_json_folder.replace(fixed_json_folder.split("/")[-1], "combined_fixed_json")
    os.makedirs(output_folder, exist_ok=True)
    dict=get_names_from_folder(fixed_json_folder,linear_json_folder)
    print(dict)
    for key, value in dict.items():
        print(key, value)
        if len(value) == 2:
            print("Found both fixed and linear JSON files for key:", key)
            combine(value[0], value[1], key,output_folder)
        else:
            print("Missing either fixed or linear JSON file for key:", key)
            os.makedirs(f"{output_folder}/{key}", exist_ok=True)
            shutil.copy(value[0], f"{output_folder}/{key}/{key}_final.json")
    return output_folder
if __name__ == "__main__":
    combine_json_files("/home/saran/POC/Applus_australia/test/json","/home/saran/POC/Applus_australia/test/linear_json")

            
            