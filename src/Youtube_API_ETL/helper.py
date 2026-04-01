from datetime import datetime
import os, json, re


def load_json(path):
    if os.path.exists(path):
        with open(path, 'r') as f:
            return json.load(f)
    return {}


def save_json(path, data):
    with open(path, 'w') as file:
        json.dump(data, file, indent=4)
        
        
def format_duration(ISO_duration):
    pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
    match = re.search(pattern, ISO_duration)
    
    if not match:
        return "0:00"
    
    h = int(match.group(1) or 0)
    m = int(match.group(2) or 0)
    s = int(match.group(3) or 0)
    
    if int(h) > 0:
        return f"{h}:{int(m):02d}:{int(s):02d}"
    return f"{m}:{int(s):02d}"

def str_to_date(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d').date()