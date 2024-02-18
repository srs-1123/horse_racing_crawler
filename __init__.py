from horse_racing_crawler import pysql
from horse_racing_crawler import df_io
from horse_racing_crawler import get_past_race
from horse_racing_crawler import Race_ver2_03

# 旧バージョンを使いたい場合はver=1に変更
ver = 2
if ver == 1:
    from horse_racing_crawler.get_past_race_ver1 import get_past_race 
    from horse_racing_crawler.get_past_race_ver1  import sort_all_race_data

if ver == 2:
    from horse_racing_crawler.get_past_race import get_past_race 
    from horse_racing_crawler.get_past_race  import sort_all_race_data

from horse_racing_crawler.pysql import PySQL
from horse_racing_crawler_crawler.df_io import read_all_data
from horse_racing_crawler.df_io import merge_umainfo
from horse_racing_crawler.df_io import read_all_umainfo
from horse_racing_crawler.df_io import to_csv
from horse_racing_crawler.Race_ver2_03 import Race_Crawler
from horse_racing_crawler.Race_ver2_03 import Payout_Crawler 
from horse_racing_crawler.Race_ver2_03 import Horse_Info_Crawler
from horse_racing_crawler.Race_ver2_03 import get_id


__version__ = '1.0.2'