import pandas as pd
import requests

#created an extract function from the api
def extract_from_API_(URL, cols,countries,jobs):
    url = "https://jsearch.p.rapidapi.com/search"
    all_data = pd.DataFrame()
    for i in countries:
        for j in jobs:
            querystring = {"query":f"{i}, {j}","page":"1","num_pages":"1"}
            headers = {
                    "X-RapidAPI-Key": "b4ae60111cmshba38a6d6440bd93p1eb62ejsn40f5b1666a74",
                    "X-RapidAPI-Host": "jsearch.p.rapidapi.com"}
            response = requests.get(url, headers=headers, params=querystring)
            response = response.json()
            data = response.get('data')
            data = pd.DataFrame(data, columns=cols)
            all_data = pd.concat([all_data, data])

