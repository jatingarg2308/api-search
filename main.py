import arrow
import re
import requests
import pandas as pd

from flask import Flask, request, abort, jsonify

from table import ingestion, get_df
from metadata import get_metadata


meta = get_metadata()


def get_video_metadata():
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

def search(x, search):
    for i in range(len(x)):
        x[i] = x[i].lower()
        x[i] = re.sub("[^a-z0-9]", "", x[i])
    for i in range(len(search)):
        search[i] = search[i].lower()

    common_words = set(x) & set(search)
    return len(common_words) != 0

def get_data():
    params = {
        'search': ';',
        'page_number': 1,
        'results_per_page': 5
    }
    start_idx = params['results_per_page'] * (params['page_number'] - 1)
    end_idx = start_idx + params['results_per_page']

    params['search'] = re.sub("[^a-z0-9]", "", params['search'])

    final_df = get_df(params)
    
    res = {
        "status_code": 200,
        "start_idx": start_idx,
        'end_idx': end_idx,
        'data': jsonify(final_df.to_json(orient="records"))
    }
    return res


if __name__ == '__main__':
    # get_video_metadata()
    res = get_data()
    import pdb; pdb.set_trace()

    