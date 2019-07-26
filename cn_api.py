import pandas as pd
import requests
import sys
from os.path import join


class CnAPI(object):

    def __init__(self):

        key, secret = 'fa4e980eaf7e4302811fb72336a648d0', '939563c8f0df4092b58823ae0d53ccb0'
        self.token = self.__get_token(key, secret)

        name_dict_file = join(sys.path[0], 'name_dictionary_cn_info.xlsx')
        self.income_dict = pd.read_excel(name_dict_file, 'income')['中文名称'].to_dict()
        self.balance_dict = pd.read_excel(name_dict_file, 'balance')['中文名称'].to_dict()
        self.cash_dict = pd.read_excel(name_dict_file, 'cash')['中文名称'].to_dict()

    def get_statement(self, code_list):

        income_url = 'http://webapi.cninfo.com.cn/api/stock/p_stock2301'
        balance_url = 'http://webapi.cninfo.com.cn/api/stock/p_stock2300'
        cash_url = 'http://webapi.cninfo.com.cn/api/stock/p_stock2302'

        post_data = {'scode': ','.join(code_list), 'type': '071001', 'source': '033003'}

        income = self.__cninfo_api(income_url, post_data).rename(columns=self.income_dict)
        balance = self.__cninfo_api(balance_url, post_data).rename(columns=self.balance_dict)
        cash = self.__cninfo_api(cash_url, post_data).rename(columns=self.cash_dict)

        try:
            statement_list = [statement.set_index(['证券代码', '证券简称', '报告年度']) for statement in [income, balance, cash]]
            drop_list = ['公告日期', '截止日期', '合并类型编码', '合并类型', '报表来源编码', '报表来源', '机构名称', '开始日期']
            return pd.concat(statement_list, axis=1).drop(columns=drop_list)
        except KeyError:
            return pd.DataFrame()

    def get_industry_lists(self):
        url, post_data = 'http://webapi.cninfo.com.cn/api/stock/p_public0004', {'platetype': '137002'}
        raw_df = self.__cninfo_api(url, post_data, result_keyword='records', dataframe=True)

        name_dict = {'F009V': 'class_code', 'F011V': 'subclass_code', 'F004V': 'class_name', 'F006V': 'subclass_name',
                     'SECCODE': 'code', 'SECNAME': 'name'}

        return raw_df.rename(columns=name_dict)[name_dict.values()].sort_values(by=['class_code', 'subclass_code'])

    def __get_token(self, key, secret):

        url = 'http://webapi.cninfo.com.cn/api-cloud-platform/oauth2/token'
        post_data = {'grant_type': 'client_credentials', 'client_id': key, 'client_secret': secret}

        the_token = self.__cninfo_api(url, post_data, result_keyword='access_token', dataframe=False)

        if the_token:
            print('token获取成功')
        else:
            print('token获取失败')

        return the_token

    def __cninfo_api(self, url, post_data, result_keyword='records', dataframe=True):

        try:
            post_data.update(access_token=self.token)
        except AttributeError:
            pass

        raw = requests.post(url=url, data=post_data).json()

        try:
            result = raw[result_keyword]
            result = pd.DataFrame(result) if dataframe else result
        except KeyError:
            print('数据获取错误，获取到的原始数据：', raw)
            result = pd.DataFrame() if dataframe else None

        return result


# engine = create_engine('mysql://root:12qwaszx^@localhost:3306/cn_info')
# nn.to_sql(name='industry', con=engine, if_exists='replace', index=False)
