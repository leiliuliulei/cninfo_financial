import pandas as pd
import requests
import sqlite3
from math import ceil
import tushare as ts
from datetime import datetime, timedelta


class CnInfoAPI:

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

        # 下载行业分类信息，并保存到数据库里
        # platetype: 137002是证监会分类，137003是巨潮分类，137004是申银万国分类，137005是新财富分类
        # 经过比较，申银万国分类最详细，最合理。把应该放到一起比较的公司放到了一起。因此这里采用申银万国分类

        url, post_data = 'http://webapi.cninfo.com.cn/api/stock/p_public0004', {'platetype': '137004'}
        raw_df = self.__cninfo_api(url, post_data, result_keyword='records', dataframe=True)

        name_dict = {'F004V': 'class_name', 'F006V': 'subclass_name', 'SECCODE': 'code', 'SECNAME': 'name'}

        if not raw_df.empty:
            cleaned_df = raw_df.rename(columns=name_dict)[name_dict.values()].sort_values(by=['class_name', 'subclass_name'])
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


class TuShareAPI:

    def __init__(self):
        self.__token = 'b19d070b1ac1e63f7b1ae4d353d109794441e4bd830e7beeb2ebd8bd'

    def download_daily_price(self, which_day=None):

        if not which_day:
            which_day = datetime.today() - timedelta(days=1)       # 如果不写哪天，就取昨天的。which_day格式: YYYYMMDD, 如 20190821

        price_df = pd.DataFrame()
        limit = 10
        while price_df.empty and limit > 0:

            price_df = ts.pro_api(self.__token).daily(trade_date=which_day.strftime('%Y%m%d'))

            which_day -= timedelta(days=1)
            limit -= 1

        replace_dict = {'ts_code': {r'\D': ''}}    # 去除code后面的.SZ, .SH 这样的字样。 \D表示“非数字”
        name_dict = {'ts_code': 'code', 'trade_date': 'date', 'close': 'price'}

        processed_df = price_df.replace(replace_dict, regex=True).rename(columns=name_dict)[name_dict.values()]

        return processed_df


class DatabaseAPI:

    def __init__(self):

        db_file, industry_table, statement_table = 'financial_statements.sqlite3', 'industry', 'statement'

        self.database = sqlite3.connect(db_file)
        self.industry_table = industry_table
        self.statement_table = statement_table

    def close(self):
        self.database.close()

    def __enter__(self):

        db_file, industry_table, statement_table = 'financial_statements.sqlite3', 'industry', 'statement'

        self.database = sqlite3.connect(db_file)
        self.industry_table = industry_table
        self.statement_table = statement_table

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.database.close()

    def get_industry(self, stock_name=None, sub_industry=None):

        sql_string = 'SELECT * FROM {}'.format(self.industry_table)

        if stock_name:
            sql_string += ' WHERE name = "{}"'.format(stock_name)
        elif sub_industry:
            sql_string += ' WHERE subclass_name = "{}"'.format(sub_industry)

        sql_result = pd.read_sql_query(sql=sql_string, con=self.database)

        try:
            class_name, subclass_name = sql_result.class_name.iloc[0], sql_result.subclass_name.iloc[0]
        except IndexError:
            class_name, subclass_name = None, None

        return class_name, subclass_name

    def get_industry_list(self):
        sql_string = 'SELECT * FROM {t}'.format(t=self.industry_table)
        return pd.read_sql_query(sql=sql_string, con=self.database).class_name.unique().tolist()

    def get_sub_industry_list(self, industry_name=None):
        sql_string = 'SELECT * FROM {t}'.format(t=self.industry_table)

        if industry_name:
            sql_string += ' WHERE class_name = "{n}"'.format(n=industry_name)

        return pd.read_sql_query(sql=sql_string, con=self.database).subclass_name.unique().tolist()

    def get_stock_list(self, industry_name=None, sub_industry_name=None):

        sql_string = 'SELECT * FROM {t}'.format(t=self.industry_table)      # 如果不输入行业、子行业，就返回全部股票名称

        if industry_name:
            sql_string += ' WHERE class_name = "{n}"'.format(n=industry_name)
        elif sub_industry_name:
            sql_string += ' WHERE subclass_name = "{n}"'.format(n=sub_industry_name)

        return pd.read_sql_query(sql=sql_string, con=self.database).name.unique().tolist()

    def get_stock_codes(self, industry_name):
        sql_string = 'SELECT * FROM {t} WHERE subclass_name = "{n}"'.format(t=self.industry_table, n=industry_name)
        return pd.read_sql_query(sql=sql_string, con=self.database).code.tolist()

    def get_statements(self, industry=None, sub_industry=None, sort_income=True, limit=10, with_price=False):

        sql_string = 'SELECT * FROM {s} s INNER JOIN {i} i on s.证券简称 = i.name'. \
            format(s=self.statement_table, i=self.industry_table)

        if industry:
            sql_string += ' WHERE i.class_name = "{}"'.format(industry)
        elif sub_industry:
            sql_string += ' WHERE i.subclass_name = "{}"'.format(sub_industry)

        statements = pd.read_sql_query(sql=sql_string, con=self.database)

        if sort_income:
            statements['一、营业总收入'] = statements['一、营业总收入'].apply(pd.to_numeric)
            statements.sort_values(by='一、营业总收入', ascending=False, inplace=True)

        if limit:
            statements = statements.iloc[:limit]

        if with_price:
            statements = statements.merge(TuShareAPI().download_daily_price(), left_on='证券代码', right_on='code')

        statements = self.__post_process(statements)

        return statements

    @classmethod
    def __post_process(cls, statement_df):
        return statement_df.drop(columns='证券代码').set_index(['证券简称', '报告年度']).apply(pd.to_numeric, errors='ignore').round(2)


class AnalysisAPI:

    def __init__(self, df_from_finance_db):
        self.df = df_from_finance_db
        self.__nice_report_period()
        self.__extra_columns()

    def income_df(self):
        needed_columns = ['一、营业总收入', '毛利润', '三、营业利润', '四、利润总额', '五、净利润']
        return self.__yi(self.df[needed_columns])

    def cost_df(self):
        needed_columns = ['剩余利润', '其他成本', '利息支出', '资产减值损失', '财务费用', '管理费用', '销售费用', '其中：营业成本']
        return self.df[needed_columns].rename(columns={'其中：营业成本': '营业成本'}).divide(self.df['一、营业总收入'], axis=0).round(2)

    def efficiency_df(self):
        needed_columns = ['毛利率', '运营利润率', '总利润率', '净利率', '股东净利率', '股东ROA', '股东ROE']
        return self.df[needed_columns].round(2)

    def nice_companies(self):
        needed_columns = ['五、净利润', '股东ROE', 'subclass_name', 'class_name', 'PE']
        d = {'subclass_name': '子行业', 'class_name': '行业', '五、净利润': '净利润'}

        renamed_df = self.df[needed_columns].reset_index().rename(columns=d)
        restricted_df = renamed_df.query('股东ROE > 0 and 净利润 > 0 and PE > 0 and 股东ROE < 1 and PE < 100')

        return restricted_df

    def __nice_report_period(self):
        d = {'-3-31': ' Q1', '-6-30': ' 半年', '-9-30': ' Q3',  '-12-31': ''}     # 优化“报告年度”的文字，利于图表呈现
        self.df = self.df.reset_index().replace({'报告年度': d}, regex=True).set_index(['证券简称', '报告年度'])

    def __extra_columns(self):

        # 运营效率图
        self.df['毛利润'] = self.df['一、营业总收入'] - self.df['二、营业总成本']
        self.df['毛利率'] = self.df['毛利润'] / self.df['一、营业总收入']

        self.df['运营利润率'] = self.df['三、营业利润'] / self.df['一、营业总收入']

        self.df['总利润率'] = self.df['四、利润总额'] / self.df['一、营业总收入']

        self.df['净利率'] = self.df['五、净利润'] / self.df['一、营业总收入']
        self.df['股东净利率'] = self.df['归属于母公司所有者的净利润'] / self.df['一、营业总收入']

        self.df['股东ROA'] = self.df['归属于母公司所有者的净利润'] / self.df['资产总计']
        self.df['股东ROE'] = self.df['归属于母公司所有者的净利润'] / self.df['所有者权益（或股东权益）合计']

        # 成本拆解图
        self.df['其他成本'] = self.df['二、营业总成本'] - self.df['其中：营业成本'] - self.df['利息支出'] - self.df['销售费用'] - self.df['管理费用'] - self.df['财务费用'] - self.df['资产减值损失']
        self.df['剩余利润'] = self.df['一、营业总收入'] - self.df['二、营业总成本']

        # atlas图
        self.df['每股收益'] = self.df['归属于母公司所有者的净利润'] / self.df['实收资本（或股本）']
        if 'price' in self.df:
            self.df['PE'] = self.df['price'] / self.df['每股收益']

    @classmethod
    def __yi(cls, a_df):
        return (a_df / 100000000).round(1)


def cut_list(the_list, limit):
    number_of_block = ceil(len(the_list) / limit)
    return [the_list[n * limit: (n + 1) * limit] for n in range(number_of_block)]
