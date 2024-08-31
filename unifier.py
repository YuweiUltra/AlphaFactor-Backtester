import requests
import pandas as pd
import json


class unifier:
    url = 'https://unifier.exponential-tech.ai/unifier'
    user = ''
    token = ''

    @classmethod
    def query(cls, name, user=None, token=None, key=None, as_of=None, back_to=None):
        headers = {
            'Content-Type': 'application/json'
        }
        payload = {
            'name': name,
            'user': cls.user,
            'token': cls.token
        }
        if key is not None:
            payload['key'] = key
        if as_of is not None:
            payload['as_of'] = as_of
        if back_to is not None:
            payload['back_to'] = back_to
        if token is not None:
            payload['token'] = token
        if user is not None:
            payload['user'] = user

        response = requests.post(cls.url, headers=headers, json=payload)
        response_data = response.json()
        if 'error' in response_data:
            print("error:", response_data['error'])
            return {}
        return response_data

    @classmethod
    def get_dataframe(cls, name, user=None, token=None, key=None, as_of=None, back_to=None):
        json_result = cls.query(name, user, token, key, as_of, back_to)
        return pd.DataFrame.from_dict(json_result)
