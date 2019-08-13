import pandas as pd
import requests
import sqlite3
from math import ceil


class CnInfoAPI(object):

    def __init__(self, key, secret):

        self.__token = self.__get_token(key, secret)

        name_dict_df = pd.read_excel('name_dictionary_cn_info.xlsx', sheet_name=None, index_col=0)

        self.__income_dict = name_dict_df['income']['中文名称'].to_dict()
        self.__balance_dict = name_dict_df['balance']['中文名称'].to_dict()
        self.__cash_dict = name_dict_df['cash']['中文名称'].to_dict()

    def __download_statement_base(self, code_list, report_period):

        # 这是基础下载模块。在这个模块里不去检查code_list是否超过50个股票代码，而是给什么下载什么。在上一级模块检查这个。

        income_url = 'http://webapi.cninfo.com.cn/api/stock/p_stock2301'
        balance_url = 'http://webapi.cninfo.com.cn/api/stock/p_stock2300'
        cash_url = 'http://webapi.cninfo.com.cn/api/stock/p_stock2302'

        post_data = {'scode': ','.join(code_list), 'type': '071001', 'source': '033003', 'rdate': report_period}
        url_dict = [[income_url, self.__income_dict], [balance_url, self.__balance_dict], [cash_url, self.__cash_dict]]
        the_index = ['证券代码', '证券简称', '报告年度']
        drop_list = ['公告日期', '截止日期', '合并类型编码', '合并类型', '报表来源编码', '报表来源', '机构名称', '开始日期']

        try:
            downloaded = [self.__cninfo_api(u, post_data).rename(columns=d)[d.values()].set_index(the_index) for u, d in url_dict]
            return pd.concat(downloaded, axis=1).drop(columns=drop_list).reset_index()
        except AttributeError:
            return pd.DataFrame()

    def download_statements(self, database_con, table_name, code_list, report_period='2018-12-31', limit=50):

        total_df_list = [self.__download_statement_base(code_piece, report_period) for code_piece in cut_list(code_list, limit)]
        total_df = pd.concat(total_df_list, sort=False)

        if not total_df.empty:
            total_df.to_sql(name=table_name, con=database_con, if_exists='replace', index=False)
            print('本次报表数据成功下载，并保存进数据库')
        else:
            print('本次报表数据下载为空，因此未保存进数据库')

    def download_industry_lists(self, database_con, table_name):
        url, post_data = 'http://webapi.cninfo.com.cn/api/stock/p_public0004', {'platetype': '137002'}
        raw_df = self.__cninfo_api(url, post_data, result_keyword='records', dataframe=True)

        name_dict = {'F009V': 'class_code', 'F011V': 'subclass_code', 'F004V': 'class_name', 'F006V': 'subclass_name',
                     'SECCODE': 'code', 'SECNAME': 'name'}
        if raw_df:
            cleaned_df = raw_df.rename(columns=name_dict)[name_dict.values()].sort_values(by=['class_code', 'subclass_code'])
            cleaned_df.to_sql(name=table_name, con=database_con, if_exists='replace', index=False)

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
            post_data.update(access_token=self.__token)
        except AttributeError:
            pass

        raw = requests.post(url=url, data=post_data).json()

        try:
            result = raw[result_keyword]
            result = pd.DataFrame(result) if dataframe else result
        except KeyError:
            print('数据获取错误，获取到的原始数据：', raw)
            result = None

        return result


class DatabaseAPI(object):

    def __init__(self):

        db_file, industry_table, statement_table = 'financial_statements.sqlite3', 'industry', 'statement'

        self.database = sqlite3.connect(db_file)
        self.industry_table = industry_table
        self.statement_table = statement_table

    def close(self):
        self.database.close()

    def get_industry(self, stock_name):

        sql_string = 'SELECT * FROM {t} WHERE name = "{n}"'.format(t=self.industry_table, n=stock_name)
        try:
            return pd.read_sql_query(sql=sql_string, con=self.database).subclass_name.tolist()[0]
        except IndexError:
            print('该股票名称不存在：{name}'.format(name=stock_name))
        return None

    def get_stock_names(self, industry_name):
        sql_string = 'SELECT * FROM {t} WHERE subclass_name = "{n}"'.format(t=self.industry_table, n=industry_name)
        return pd.read_sql_query(sql=sql_string, con=self.database).name.tolist()

    def get_stock_codes(self, industry_name):
        sql_string = 'SELECT * FROM {t} WHERE subclass_name = "{n}"'.format(t=self.industry_table, n=industry_name)
        return pd.read_sql_query(sql=sql_string, con=self.database).code.tolist()

    def get_all_industry(self):
        sql_string = 'SELECT * FROM {t}'.format(t=self.industry_table)
        return pd.read_sql_query(sql=sql_string, con=self.database).subclass_name.unique().tolist()

    def get_statement(self, names, whole_industry=False, sort_income=True):

        if whole_industry:
            name = names if isinstance(names, str) else names[0]                    # 取stock_names的第一个
            names = self.get_stock_names(industry_name=self.get_industry(name))     # 找到它所属行业的所有公司

        if isinstance(names, str):
            condition = '"证券简称"="{n}"'.format(n=names)
        elif isinstance(names, list):
            condition = ' or '.join(['"证券简称"="{n}"'.format(n=stock_name) for stock_name in names])
        else:
            condition = ''

        sql_string = 'SELECT * FROM {t} WHERE '.format(t=self.statement_table) + condition
        statements = pd.read_sql_query(sql=sql_string, con=self.database)

        if whole_industry and sort_income:
            statements.sort_values(by='一、营业总收入', ascending=False, inplace=True)

        return statements


def cut_list(the_list, limit):
    number_of_block = ceil(len(the_list) / limit)
    return [the_list[n * limit: (n + 1) * limit] for n in range(number_of_block)]
