import time
import requests
import pandas as pd

from table import ingestion, create_database, create_table
from metadata import get_metadata


meta = get_metadata()
KEY_INDEX = 0

def get_video_metadata():
    global meta, KEY_INDEX
    params = {
        "part" : 'snippet',
        "type" : "video",
        "order": 'date',
        "q": "|".join(meta['search_query']),
        "publishedAfter": "2021-06-16T00:00:00Z",
        "key": meta['youtube_keys'][KEY_INDEX],
        "maxResults": 10
    }

    url = meta['youtube_url'] + "?"
    for param in params.keys():
        url += f"{param}={params[param]}&"
    metadata_res = requests.get(
                    url=url[:-1],
                )
    if metadata_res.status_code > 200:
        print('All Units exhauted')
        KEY_INDEX = (KEY_INDEX+1)%len(meta['youtube_keys'])
    else:
        df = pd.DataFrame(columns =[col['name'] for col in meta['col_metadata']])
        for metadata in metadata_res.json()['items']:
            data = {
                'video_id': metadata['id']['videoId'],
                'title': metadata['snippet']['title'],
                'description': metadata['snippet']['description'],
                'publish_time': metadata['snippet']['publishTime'],
                'thumbnails': str(metadata['snippet']['thumbnails'])
            }
            df = df.append(data, ignore_index=True)
        
        ingestion(df)

def video_metadata_loop():
    while True:
        time.sleep(10)
        get_video_metadata()

    

if __name__ == '__main__':
    create_database()
    create_table()
    video_metadata_loop()
