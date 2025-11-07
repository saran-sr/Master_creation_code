import os,shutil
from Insert.fixed_master import load_config


def upload_fixed_images(json_folder):
    config=load_config()
    image_directory=config['image_directory']
    site_id=str(config['site_id'])
    
    url="/run/user/1000/gvfs/smb-share:server=anton.local,share=sr-storage/SeekRight"
    full_image_directory=os.path.join(url,image_directory)
    if not os.path.exists(full_image_directory):
        os.makedirs(full_image_directory,exist_ok=True)
    for root, dirs, files in os.walk(json_folder):
        for file in files:
            if file.endswith(".jpeg") and file.startswith(site_id):
                path=os.path.join(root, file)
                print(path, full_image_directory+file)
                src=path
                dst=os.path.join(full_image_directory,file)
                with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
                    shutil.copyfileobj(fsrc, fdst)