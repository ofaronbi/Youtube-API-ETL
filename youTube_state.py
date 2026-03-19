import requests, json, os
from  datetime import date, datetime
from dotenv import load_dotenv


load_dotenv(dotenv_path="./.env")

YOUR_API_KEY = os.getenv("YOUR_API_KEY")
CHANNEL_HANDLER = os.getenv("CHANNEL_HANDLER")


def get_playlist_id():
    with open('Youtube-API-ETL/api_result.json', 'r+') as file:
        data = json.load(file)
        last_call_date = datetime.strptime(data['call_date'], '%Y-%m-%d').date()
        # print(f'last call date: {last_call_date}')
        current_date = date.today()
        # print(f'current date:{current_date}')
        if current_date > last_call_date:
            try:
                url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLER}&key={YOUR_API_KEY}'
                response = requests.get(url)
                response.raise_for_status()
                new_data = response.json()
                new_data['call_date'] = str(current_date)
                file.seek(0)
                file.truncate()
                json.dump(new_data, file, indent=4)
                data = new_data 
            except:
                raise requests.exceptions.RequestException
        else:
            print('Date is still current.')
            
        if data['items'][0]:
            channel_playlistid = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            return channel_playlistid
        else:
            return "Error: No channel found for that handle."
            
                
if __name__ == "__main__":   
    result = get_playlist_id()
    print(result)