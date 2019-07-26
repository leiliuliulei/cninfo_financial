from os.path import join
import pandas as pd
import sys
from cn_api import CnAPI


industry = pd.read_csv(join(sys.path[0], 'industry.csv'), dtype='str')

one_download = CnAPI()
one_download.get_statement(['600354']).to_csv(join(sys.path[0], '600354.csv'))
# one_industry = industry.groupby('subclass_name').get_group('专业技术服务业').code.tolist()
# one_download.get_statement(one_industry).to_csv(join(sys.path[0], '专业技术服务业.csv'))
#
# one_industry = industry.groupby('subclass_name').get_group('农业').code.tolist()
# one_download.get_statement(one_industry).to_csv(join(sys.path[0], '农业.csv'))
