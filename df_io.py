# df_io.py
#----------------------------------------------------------------------------
#!/usr/bin/env python3 
# -*- coding: utf-8 -*- 
#----------------------------------------------------------------------------
# Created By  : Shirasukazushi
# Created Date: 2022/06/25
# ---------------------------------------------------------------------------
# 関数
#   read_all_data   : すべてのレースデータを取得(日にちと馬番号でソート，インデックス振りなおし)
#   to_csv          :データフレームを特定のフォルダに保存
# ---------------------------------------------------------------------------
# Imports 
# ---------------------------------------------------------------------------
import os
import pandas as pd
from time import sleep

def read_all_data(start_year, end_year, input_dir="race_csv_data"):
    """各年のデータをデータフレームとして読み込み，リストにする

    Returns:
        list_df_race: list
            各年のレースデータのリスト
    """
    dir_ = os.getcwd().replace(os.sep,'/') # カレントディレクトリを取得
    copy_flag = True
    df_all_race = None
    years = [year for year in range(start_year, end_year+1)]
    
    for year in years:
        # 1年分のレースデータを読み込む
        df_race = pd.read_csv('{}/{}/{}_all_race.csv'.format(dir_, input_dir, year),encoding='shift-jis')
        
        # データを１つにまとめる
        if copy_flag:
            df_all_race = df_race.copy()
            copy_flag = False
        else:
            df_all_race = pd.concat([df_all_race, df_race])
            
        # 進行状況を出力
        # print("\r{}年 レース数 : {}".format(year, len(df_race.Race_Id.drop_duplicates())), end='')
        print("\r{}年, 計{}レース".format(year, len(df_all_race.Race_Id.drop_duplicates())), end="")
        # sleep(2)
    # pd.to_pickle(df_all_race, 'df_all_race.pkl')
    
    # Date列に時間を追加
    df_all_race.Date = pd.to_datetime(df_all_race.Date)
    
    # 日にち，馬番号でソート
    df_all_race = df_all_race.sort_values(by=["Date", "Number"]).reset_index(drop=True)
    
    return df_all_race

def merge_umainfo(start_year, end_year=None, output_dir="race_csv_data_with_umainfo"):
    """各年のデータをデータフレームとして読み込み，リストにする

    Returns:
        list_df_race: list
            各年のレースデータのリスト
    """
    dir_ = os.getcwd().replace(os.sep,'/') # カレントディレクトリを取得
    if end_year == None:
        end_year = start_year
    years = [year for year in range(start_year, end_year+1)]

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    # df_umainfo = read_all_umainfo(2000, end_year, input_dir="umainfo_csv_data")
    df_umainfo = read_all_umainfo(2000, 2022, input_dir="umainfo_csv_data")
    
    for year in years:
        # 1年分のレースデータを読み込む
        df_race = pd.read_csv('{}/race_csv_data_with_past_race_data/{}_all_race.csv'.format(dir_, year),encoding='shift-jis')
        df_merged_race = pd.merge(df_race, df_umainfo, on="Uma_Id", how="left")
        df_merged_race.to_csv('{}/{}/{}_all_race.csv'.format(dir_, output_dir, year), encoding = "shift-jis",index = False)
            
        # 進行状況を出力
        print("\r{}年".format(year), end="")
        # sleep(2)

def read_all_umainfo(start_year, end_year, input_dir="umainfo_csv_data"):
    """各年のデータをデータフレームとして読み込み，リストにする

    Returns:
        list_df_race: list
            各年のレースデータのリスト
    """
    dir_ = os.getcwd().replace(os.sep,'/') # カレントディレクトリを取得
    copy_flag = True
    df_all_race = None
    years = [year for year in range(start_year, end_year+1)]
    
    for year in years:
        # 1年分のレースデータを読み込む
        df = pd.read_csv('{}/{}/{}.csv'.format(dir_, input_dir, year),encoding='shift-jis')
        
        # データを１つにまとめる
        if copy_flag:
            df_all_race = df.copy()
            copy_flag = False
        else:
            df_all_race = pd.concat([df_all_race, df])
            
        # 進行状況を出力
        # print("\r{}年 レース数 : {}".format(year, len(df_race.Race_Id.drop_duplicates())), end='')
        sleep(1)
        print("\r{}年, 計{}頭".format(year, len(df_all_race.Uma_Id.drop_duplicates())), end="")
        # sleep(2)
    # pd.to_pickle(df_all_race, 'df_all_race.pkl')
    
    return df_all_race.loc[:, :'M_Mother_Id']

def to_csv(df, filename, output_dir="preprocessed_csv_data"):
    dir_ = os.getcwd().replace(os.sep,'/') # カレントディレクトリを取得
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    df.to_csv('{}/{}/{}'.format(dir_, output_dir, filename), encoding = "shift-jis",index = False)