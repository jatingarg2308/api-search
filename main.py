import arrow
import requests
import json
import time
import pandas as pd

from flask import Flask, request, jsonify
from multiprocessing import Process


from table import ingestion, get_df, create_database, create_table
from metadata import get_metadata

app = Flask(__name__)
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
        get_video_metadata()
        time.sleep(10) 

@app.route('/', methods=['GET'])
def get_data():

    params = request.json
    start_idx = params['results_per_page'] * (params['page_number'] - 1)
    end_idx = start_idx + params['results_per_page']

    final_df = get_df(params)
    
    res = {
        "status_code": 200,
        "start_idx": start_idx,
        'end_idx': end_idx,
        'data': json.loads(final_df.to_json(orient="records"))
    }
    return res
    


if __name__ == '__main__':
    create_database()
    create_table()
    # get_video_metadata()
    p = Process(target=video_metadata_loop, args= ()) 
    p.start()
    app.run(port=5000, use_reloader=False)
    p.join()



    
    

    