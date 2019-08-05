from os.path import join
import sys
from time import sleep
from cn_api import CnAPI

liu_lei_key, liu_lei_secret = 'fa4e980eaf7e4302811fb72336a648d0', '939563c8f0df4092b58823ae0d53ccb0'
liu_lei = CnAPI(liu_lei_key, liu_lei_secret)

for one_industry in liu_lei.industry_list:

    codes_in_this_industry = liu_lei.industry_to_codes(one_industry)

    print(one_industry, '数量：', len(codes_in_this_industry))
    print('\t', '开始下载')

    result = liu_lei.download_statements(code_list=codes_in_this_industry)

    if not result.empty:
        save_dir = join(sys.path[0], 'financial_data')
        save_file = join(save_dir, one_industry + '.csv')
        result.to_csv(save_file)
        print('\t', '下载完成')
    else:
        print('\t', '下载失败')

    sleep(1)
