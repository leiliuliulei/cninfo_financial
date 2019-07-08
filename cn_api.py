import pandas as pd
import requests


class CnAPI(object):

    def __init__(self):
        self.token = self.get_token()

    @classmethod
    def get_token(cls):
        url = 'http://webapi.cninfo.com.cn/api-cloud-platform/oauth2/token'
        key, secret = 'fa4e980eaf7e4302811fb72336a648d0', '939563c8f0df4092b58823ae0d53ccb0'
        post_data = {'grant_type': 'client_credentials', 'client_id': key, 'client_secret': secret}

        return cls.cninfo_request(url, post_data, result_keyword='access_token', dataframe=False)

    @classmethod
    def cninfo_request(cls, url, post_data, result_keyword, dataframe=False):

        raw = requests.post(url=url, data=post_data).json()
        result = pd.DataFrame(raw[result_keyword]) if dataframe else raw[result_keyword]

        return result


ll = CnAPI()
