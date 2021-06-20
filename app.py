import json
import time

from flask import Flask, request, jsonify

from table import get_df


app = Flask(__name__)


@app.route('/', methods=['GET'])
def get_data():

    params = request.json
    # params = {
        # "search": "",
        # "page_number": 2,
        # "results_per_page": 5
    # }
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
    app.run(port=5000, use_reloader=False, host="0.0.0.0")
    



    
    

    