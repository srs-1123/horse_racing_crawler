# get_past_race.py
#----------------------------------------------------------------------------
#!/usr/bin/env python3 
# -*- coding: utf-8 -*- 
#----------------------------------------------------------------------------
# Created By  : Shirasukazushi
# Created Date: 2022/06/23
# ---------------------------------------------------------------------------
# 関数
#   get_past_data    : 過去データを取得，Date列に時間を追加
# ---------------------------------------------------------------------------
# 実行方法
#   get_past_raceフォルダを丸ごとgoogle driveからダウンロード
#   vsコードでget_past_raceフォルダを開き，このファイルを実行
# ---------------------------------------------------------------------------
# 注意点
#   csvファイルで保存するとdatetime型がobject型に勝手に変換されるみたい
#   sorted_all_race_data.csv : すべての年のレースを馬，日付で並べ替えたデータ
# ---------------------------------------------------------------------------
# 初期環境構築：
#   pip3 install tqdm
# ---------------------------------------------------------------------------
# Imports 
# ---------------------------------------------------------------------------
import os
import pandas as pd
if __name__ == '__main__':
    from tqdm import tqdm # vscodeで使う場合
else:
    from tqdm.notebook import tqdm # jupyterで使う場合

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
        print("{}年 : {}レース".format(year, len(df_race.Race_Id.drop_duplicates())))
        # sleep(2)
    # pd.to_pickle(df_all_race, 'df_all_race.pkl')
    
    # Date列に時間を追加
    df_all_race.Date = pd.to_datetime(df_all_race.Date + " "+ df_all_race.Start_Time)
    
    # 日にちでソート
    df_all_race = df_all_race.sort_values("Date").reset_index(drop=True)
    
    # 合計レース数を出力
    print("======================")
    print("計{}レース".format(len(df_all_race.Race_Id.drop_duplicates())))
    return df_all_race

def sort_all_race_data(columns, start_year=2000, end_year=2022):
    """すべてのレースを日付でソート，使う特徴量だけ抽出して出力

    Parameters
    ----------
    columns
        抽出する特徴量
    """
    df_race = read_all_data(start_year, end_year)
    grouped_race = df_race.sort_values("Date", ascending=False)[columns]
    dir_ = os.getcwd().replace(os.sep,'/')
    grouped_race.to_csv("{}/race_csv_data/sorted_all_race_data.csv".format(dir_), encoding = "shift-jis",index = False)

def get_past_race(year, columns, iter=None):
    """過去データを取得，Date列に時間を追加

    Parameters
    ----------
    year : int
        過去データを取得する年
    columns : list
        過去レースを取得する特徴量
    """
    # カレントディレクトリを取得
    dir_ = os.getcwd().replace(os.sep,'/')
    # 1年分のレースデータを読み込む
    df_race = pd.read_csv('{}/race_csv_data/{}_all_race.csv'.format(dir_, year),encoding='shift-jis')
    # 馬ごとにグループ化したデータを読みこむ(2000年～2022年)
    grouped_race = pd.read_csv('{}/race_csv_data/sorted_all_race_data.csv'.format(dir_),encoding='shift-jis')

    # Date列に時間を追加
    df_race.Date = pd.to_datetime(df_race.Date + " "+ df_race.Start_Time)
    grouped_race.Date = pd.to_datetime(grouped_race.Date)

    # df_raceをソート
    df_race = df_race.sort_values(by=["Date", "Number"]).reset_index(drop=True)
    
    # 過去レースの列をあらかじめ作っておく
    for j in range(0, 5):
        for column in columns:
            df_race["past_{}_{}".format(column, j+1)] = None
    
    # メインの処理
    if iter is None:
        end = df_race.shape[0]
    else:
        end = iter

    # プログレスバーを表示
    bar = tqdm(total = end)
    # 説明文を追加
    bar.set_description('{}年 過去データ取得中'.format(year))

    for i in range(end):
        # 1列目の馬の全レースを取得，日付でソート
        horse_name = df_race.loc[i, "Name"]
        past_race = grouped_race[grouped_race["Name"]==horse_name]

        # 1列目の馬の過去5レースを取得
        past_5_race = past_race[past_race.Date < df_race.loc[i, "Date"]].head(5).reset_index(drop=True)

        # 過去レースを追加
        for j in range(0, 5):
            for column in columns:
                try:
                    if column == "Jockey":
                        if df_race.loc[i, column] == past_5_race.loc[j, column]:
                            df_race.loc[i, "past_{}_{}".format(column, j+1)] = 1
                            # Jockeyが変わっていないとき1を代入できているか確認
                            # df_race.loc[i, "past_{}_{}".format(column, j+1)] = past_5_race.loc[j, column]
                        else:
                            df_race.loc[i, "past_{}_{}".format(column, j+1)] = 0
                    else:
                        df_race.loc[i, "past_{}_{}".format(column, j+1)] = past_5_race.loc[j, column]
                except:
                    pass # 過去レースがない場合はNoneのまま
        bar.update(1) # レースカウントを更新
    
    # 出力フォルダの指定
    output_dir = "race_csv_data_with_past_race_data"
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    df_race.to_csv('{}/{}/{}_all_race.csv'.format(dir_, output_dir, year), encoding = "shift-jis",index = False)

if __name__ == '__main__':
    # iter指定で，iter頭分だけ過去レース取得(コートをテストで動かしたいときに使う)
    # 年数のみ指定ですべての過去レース取得
    get_past_race(2022, iter=50)