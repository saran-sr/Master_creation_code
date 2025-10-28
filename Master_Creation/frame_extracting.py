import cv2
import json
import ast
import re
import glob
import os 
import pandas as pd
import shutil
import json
def get_assets_dict():
    """Returns the assets dictionary for name mapping."""
    return {
        "Traffic_Cone": "Traffic_Cone"
    }
def clean_asset_name(asset_name):
    """Clean and standardize asset names according to the original logic."""
    asset_name = asset_name.replace('LEFT_', '')
    asset_name = asset_name.replace('RIGHT_', '')
    
    asset_name = asset_name.replace("_", " ")
    return asset_name

def extract_frame(video_file, output_file, frame_number, bbox_min, bbox_max, asset_name):
    """Extract frame from video and add bounding box with label."""
    asset_name = clean_asset_name(asset_name)
    print(video_file)
    if not os.path.exists(video_file):
        #exit if video file does not exist
        print("Video file does not exist:", video_file)
    cap = cv2.VideoCapture(video_file)
    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number) 
    ret, frame = cap.read()
    
    if ret:
        print("Frame extracted successfully") ##debug
        x1, y1 = bbox_min[0], bbox_min[1]
        x2, y2 = bbox_max[0], bbox_max[1]
        cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)

        font = cv2.FONT_HERSHEY_DUPLEX
        font_scale = 1
        font_thickness = 2
        (text_width, text_height), baseline = cv2.getTextSize(asset_name, font, font_scale, font_thickness)

        padding = 5

        # Determine if label can be drawn above or below the bounding box
        label_above_box = (y1 - text_height - baseline - 10 - padding) >= 0
        if label_above_box: 
            # Draw label above the bounding box
            background_y1 = max(y1 - text_height - baseline - 10 - padding, 0)
            background_y2 = y1 - 10 + padding
            text_y = y1 - 10 - baseline
        else:  
            # Draw label below the bounding box
            background_y1 = y2 + 10
            background_y2 = min(y2 + text_height + baseline + 10 + padding, frame.shape[0])
            text_y = background_y1 + text_height

        # Calculate label width including padding
        label_width = text_width + 2 * padding
        
        # Check if label fits when positioned at x1
        if x1 + label_width <= frame.shape[1]:
            # Label fits at x1 position
            background_x1 = x1
            background_x2 = x1 + label_width
        else:
            # Label doesn't fit at x1, try positioning at x2 (right edge of bbox)
            if x2 - label_width >= 0:
                # Position label to the left, ending at x2
                background_x1 = x2 - label_width
                background_x2 = x2
            else:
                # Label is too long for the image, position at right edge of image
                background_x2 = frame.shape[1]
                background_x1 = max(0, frame.shape[1] - label_width)

        # Draw the background rectangle for the label
        cv2.rectangle(frame, (background_x1, background_y1), (background_x2, background_y2), (0, 255, 0), cv2.FILLED)

        # Draw the text within the label
        text_x = background_x1 + padding
        cv2.putText(frame, asset_name, (text_x, text_y), font, font_scale, (255, 255, 255), font_thickness, cv2.LINE_AA)

        # Save the frame with the label
        cv2.imwrite(output_file, frame)

        # Resize the image to desired dimensions
        img = cv2.imread(output_file)
        img = cv2.resize(img, (2560, 1440))

        # Save the resized image
        cv2.imwrite(output_file, img)
    else:
        print("Error: Frame not extracted")

    cap.release()

def process_json_file(file, video_path, assets_dict):
    """Process a single JSON file and extract frames for assets containing 'start' or 'end'."""
    output_lists = []
    print(file)
    
    video_name = file.split("/")[-2]
    video_file = video_path + "/" + video_name + ".MP4"
    
    f = open(file, 'r')
    f = f.read()
    data1 = ast.literal_eval(f)
    
    assets = []
    
    for i in range(len(data1['Assets'])):
        try:
            asset_name = data1['Assets'][i][0]
            if "start" not in asset_name.lower() and "end" not in asset_name.lower():
                count = data1['Assets'][i][1]
                frame = data1['Assets'][i][2]
                bbox_min = data1['Assets'][i][3]
                bbox_max = data1['Assets'][i][4]
                output_file_path = file.replace("_final.json", "")
                output_file = output_file_path + f"_{frame}_{asset_name}_{count}.jpeg"
                
                print(output_file)
                print(video_file)
                
                if not os.path.exists(output_file):
                    output_lists.append(output_file)
                    print("Missing", output_file)
                    print(asset_name)
                    
                    asset_name = asset_name.replace("LEFT_", "").replace("RIGHT_", "")
                    if asset_name in assets_dict:
                        asset_name = assets_dict[asset_name]
                        print("New asset name", asset_name)
                    
                    print(video_file, output_file, frame, bbox_min, bbox_max, asset_name)
                    extract_frame(video_file, output_file, frame, bbox_min, bbox_max, asset_name)
                else:
                    print("exists")
        except Exception as e:
            print("ex", e)
    
    return output_lists

def backup_json_folder(json_folder):
    """Create a backup of the JSON folder."""
    dest_dir = json_folder + "_backup"
    files = os.listdir(json_folder)
    shutil.copytree(json_folder, dest_dir, dirs_exist_ok=True)

def extract_asset_frames():

    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
    config_file = os.path.normpath(config_path)
    with open(config_file) as f:
        config = json.load(f)
    """Main function to process all JSON files and extract frames."""
    # video_path = r"/run/user/1000/gvfs/smb-share:server=anton.local,share=machinelearning/POC/Kazakhstan/Semey City_2025/LHS"
    # json_folder = "/media/saran/2A70ABF170ABC1C5/POC/Khazakhstan/test"
    video_path = config["video_folder"]
    json_folder = config["json_folder"]
    assets_dict = get_assets_dict()
    output_lists = []
    
    for file in glob.glob(json_folder + "/**/*.json"):
        file_output_lists = process_json_file(file, video_path, assets_dict)
        output_lists.extend(file_output_lists)
    
    backup_json_folder(json_folder)

if __name__ == "__main__":
    extract_asset_frames()