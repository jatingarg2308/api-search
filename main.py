import arrow
import asyncio
import requests
import json
import pandas as pd

from flask import Flask, request, abort, jsonify

from table import ingestion, get_df, create_database, create_table
from metadata import get_metadata

app = Flask(__name__)
meta = get_metadata()


async def get_video_metadata():
    global meta
    params = {
        "part" : 'snippet',
        "type" : "video",
        "order": 'date',
        "q": "|".join(meta['search_query']),
        "publishedAfter": "2021-06-16T00:00:00Z",
        "key": "AIzaSyB2hnV0MZJMZWTv1PUKZrpseHtKICLSo74",
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
        
        await ingestion(df)

def search(x, search):
    for i in range(len(x)):
        x[i] = x[i].lower()
        x[i] = re.sub("[^a-z0-9]", "", x[i])
    for i in range(len(search)):
        search[i] = search[i].lower()

    common_words = set(x) & set(search)
    return len(common_words) != 0

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

def video_metadata_loop():
    while True:
        get_video_metadata()
        time.sleep(100) 
    return 'hello'

async def main():
    create_database()
    create_table()

    loop = asyncio.get_event_loop()
    task1 = loop.create_task(video_metadata_loop)
    task2 = loop.create_task(app.run(port=5000))
    
    await task2
    await task1

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    
    

    