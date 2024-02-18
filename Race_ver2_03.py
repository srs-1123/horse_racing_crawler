# Race_ver2_03.py
#----------------------------------------------------------------------------
#!/usr/bin/env python3 
# -*- coding: utf-8 -*- 
#----------------------------------------------------------------------------
# Edited By  : Shirasukazushi
# Edited Date: 2020/06/22
# version ='2.0.3'
# ---------------------------------------------------------------------------
# クラス
#   Crawler            : もとになるクラス(単体での実行不可能)
#   Race_Crawler       : レース情報を取得
#   Payout_Crawler     : 払い戻し情報を取得
#   Horse_Info_Crawler : 馬情報を取得
# ---------------------------------------------------------------------------
# 関数
#   int_    : int()関数の代わりに使用
#   get_id  : jockey_id, owner_id, trainer_id, uma_idを取得
# ---------------------------------------------------------------------------
# 変更点
#   Horse_Info_Crawlerを追加
# ---------------------------------------------------------------------------
# 注意点
#   クラスはカレントディレクトリにrace_idフォルダを入れて実行
#   get_id()はカレントディレクトリにrace_csv_dataを入れて実行
# ---------------------------------------------------------------------------
# 初期環境構築：
#   pip3 install BeautifulSoup4
#   pip3 install tqdm
# ---------------------------------------------------------------------------
# Imports 
# ---------------------------------------------------------------------------
import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import datetime
from time import sleep
if __name__ == '__main__':
    from tqdm import tqdm # vscodeで使う場合
else:
    from tqdm.notebook import tqdm # jupyterで使う場合

# ---------------------------------------------------------------------------
# Crawler
# ---------------------------------------------------------------------------

class Crawler:
    def __init__(self, sleep_time=1, if_exception="pass", input_dir=None, output_dir=None):
        """netkeibaからスクレイピングを行うクラス（単体では実行不可能）

        Attributes:
        ----------
        sleep_time : int, default 1
            id毎に停止する時間
        if_exception : str, default "pass"
            例外が出た時の処理
        current_dir : str
            現在のディレクトリ
        input_dir : str
            idが保存されているフォルダ名
        output_dir : str
            csvファイルを出力するフォルダ名

        Notes
        -----
        if_exception = "pass"  -> 例外が出た時passしてそのrace_idを記録する
        if_exception = "raise" -> 例外が出た時エラーを出力
        """

        self.sleep_time = sleep_time 
        self.if_exception = if_exception 
        self.current_dir = os.getcwd().replace(os.sep,'/')
        self.input_dir = input_dir
        self.output_dir = output_dir

    def __call__(self, *filenames):
        """メインの処理

        Attributes:
        ----------
        filename : str, int
            ファイル名
        
        Examples:
        ----------
        読み込みたいファイルがtest.txt, 2020.txtの時
            test.txtの時  -> crawler("test")
            2020.txtの時  -> crawler(2020)
        """
        for filename in filenames:
            self.get_one_year_race_data(filename)

    def get_one_id_race_data(self, id=None) -> list:
        """id一つ分のレースデータを取得する

        Parameters
        ----------
        id : int, default None
            レースid

        Returns
        -------
            レース情報のリストを出力

        Raises
        ------
        NotImplementedError
            オーバーライドして使用しないとエラー
        """

        raise NotImplementedError()
    
    def get_one_year_race_data(self, filename):
        """1年分のレースデータを取得

        Parameters
        ----------
        filename : str
            idが記載されたテキストファイル名
        """

        if type(filename) is str:
            filename = filename.replace(".txt","")

        self.race_data = [] # 最終的に出力するレースデータ

        with open("{}/{}/{}.txt".format(self.current_dir, self.input_dir, filename)) as f:
            id_list = [s.strip() for s in f.readlines()]
            all_race_count = len(id_list)
        #print("filename : {}.txt".format(filename))

        # プログレスバーを表示
        bar = tqdm(total = all_race_count)
        # 説明文を追加
        bar.set_description('{}.txt'.format(filename))

        self.false_id = [] # 上手くスクレイピング出来なかったレースidのリスト  
        for id in id_list:
            self.id = id
            if self.if_exception == "pass":
                """例外が出た時passしてそのrace_idを記録する方式"""
                try:
                    data = self.get_one_id_race_data()
                except:
                    data = [] # 例外が出た場合は何も追加しない
                    self.false_id.append(self.id) # 例外が出た時そのidを記録して次のidでスクレイピング続行
                    print(self.id)

            
            if self.if_exception == "raise":
                """例外が出た時エラーを出す方式"""
                data = self.get_one_id_race_data()

            self.race_data.extend(data) # self.race_dataに全レースの情報をまとめる

            bar.update(1) # レースカウントを更新
            sleep(self.sleep_time) # self.sleep_time秒だけ停止

        # データフレーム化
        self.race_data = pd.DataFrame(self.race_data) 

        # 結果を出力
        self.output(filename)

        # 例外データのidをテキストファイルとして出力
        if self.if_exception == "pass":
            self.get_error_id(filename)

    def output(self, output_filename):
        """取得したデータを出力

        Parameters
        ----------
        output_filename : str, int
            出力ファイル名
        """
        # csvへの保存
        if not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)
        try:
            self.race_data.to_csv("{}/{}/{}.csv".format(self.current_dir, self.output_dir, output_filename), encoding="shift-jis",index = False)
        except:
            print("Saving to csv file failed.")

    def get_error_id(self, filename):
        if not self.false_id:
            pass
        else:
            file_path = "{}/{}/error_{}.txt".format(self.current_dir, self.input_dir, filename) # ファイルパス
            obj = map(lambda x: x + "\n", self.false_id) # 各要素に改行コードを付加
            # データの保存
            with open(file_path, "w", encoding="utf-8") as f:
                f.writelines(obj)
            print("\r{} exceptions were identified\n".format(len(self.false_id)), end="")

# ---------------------------------------------------------------------------
# Race_Crawler
# ---------------------------------------------------------------------------

class Race_Crawler(Crawler):
    def __init__(self, sleep_time=1, if_exception="pass", get_id=False, input_dir="race_id", output_dir="race_csv_data"):
        """レース情報をスクレイピングするクラス

        Attributes:
        ----------
        sleep_time : int, default 1
            id毎に停止する時間
        if_exception : str, default "pass"
            例外が出た時の処理
        get_id : bool, default False
            idを取得するかどうか
        current_dir : str
            現在のディレクトリ
        input_dir : str
            idが保存されているフォルダ名
        output_dir : str
            csvファイルを出力するフォルダ名

        Notes
        -----
        if_exception = "pass"  -> 例外が出た時passしてそのrace_idを記録する
        if_exception = "raise" -> 例外が出た時エラーを出力
        """
        super().__init__(sleep_time, if_exception, input_dir, output_dir)
        self.get_id_ = get_id
    
    def get_one_year_race_data(self, filename):
        """1年分のレースデータを取得

        Parameters
        ----------
        filename : str
            idが記載されたテキストファイル名
        """
        super().get_one_year_race_data(filename)
        # idを取得する
        if self.get_id_:
            self.get_id(filename)
        
    def get_one_id_race_data(self, id=None) -> list:
        """id一つ分のレースデータを取得する

        Parameters
        ----------
        id : int, default None
            レースid

        Returns
        -------
        data : list
            レース情報のリスト
        """
        if id is not None:
            self.id = id
        data = []
        # レース情報を取得
        uma_table, race_info = self.get_race_info()
        # 馬情報(元のコードでいうdetails)を取得する
        for uma_list in uma_table[1:]:
            details = self.get_detail(uma_list)
            details.update(race_info) # 馬情報とレース情報を連結
            data.append(details) # １レース分のデータを一つのリストにまとめる
        return data

    def get_race_info(self):
        """レース情報を取得

        Returns
        -------
        uma_table : list
            馬名のリスト
        race_info : dict
            レース情報の辞書
        """
        race_info = {}
        race_url = "https://db.netkeiba.com/race/" + self.id + "/"

        race_html =  requests.get(race_url)
        race_html.encoding = "EUC-JP"
        race = BeautifulSoup(race_html.text, 'html.parser')

        #日付
        date = race.find(class_="race_place fc").find(class_="result_link").find("a").get("href").split("/")
        date = datetime.datetime.strptime(date[3],"%Y%m%d")
        #会場
        place = race.find(class_="data_intro").text
        #会場id
        if "札幌" in place:
            place = "札幌"
            place_id = 0 
        elif "函館" in place:
            place = "函館"
            place_id = 1
        elif "福島" in place:
            place = "福島"
            place_id = 2
        elif "中山" in place:
            place = "中山"
            place_id = 3
        elif "東京" in place:
            place = "東京"
            place_id = 4
        elif "新潟" in place:
            place = "新潟"
            place_id = 5
        elif "中京" in place:
            place = "中京"
            place_id = 6
        elif "京都" in place:
            place = "京都"
            place_id = 7
        elif "阪神" in place:
            place =  "阪神"
            place_id = 8
        elif "小倉" in place:
            place = "小倉"
            place_id = 9
        else:
            place = ""
            place_id = 10
        #レース数
        race_num = race.find(class_="race_num fc").find("ul").find(class_="active").text.replace("R","")
        race_num = int_(race_num)
        #レース名
        race_class = race.find(class_="data_intro").find("h1").text
        race_class_sub = race.find(class_="data_intro").find(class_="smalltxt").text
        #レースid
        #race_id = race_id
        #クラス
        if "障害" in race_class or "障害" in race_class_sub:
            class_txt = "障害"
            class_id = 0
        elif "G1" in race_class or "G1" in race_class_sub:
            class_txt = "G1"
            class_id = 10
        elif "G2" in race_class or "G2" in race_class_sub:
            class_txt = "G2"
            class_id = 9
        elif "G3" in race_class or "G3" in race_class_sub:
            class_txt = "G3"
            class_id = 8
        elif "(L)" in race_class or "オープン" in race_class or "(L)" in race_class_sub or "オープン" in race_class_sub:
            class_txt = "オープン"
            class_id = 7
        elif "3勝" in race_class or "1600" in race_class or "3勝" in race_class_sub or "1600" in race_class_sub:
            class_txt = "3勝"
            class_id = 6
        elif "2勝" in race_class or "1000" in race_class or "2勝" in race_class_sub or "1000" in race_class_sub:
            class_txt = "2勝"
            class_id = 5
        elif "1勝" in race_class or "500" in race_class or "1勝" in race_class_sub or "500" in race_class_sub:
            class_txt = "1勝"
            class_id = 4
        elif "新馬" in race_class or "新馬" in race_class_sub:
            class_txt = "新馬"
            class_id = 3
        elif "未勝利" in race_class or "未勝利" in race_class_sub:
            class_txt = "未勝利"
            class_id = 2
        else:
            class_txt = ""
            class_id = 1
        #芝，ダート
        field = race.find(class_="data_intro").find("dl").find("dd").find("p").text
        if "芝" in field:
            field = "芝"
            field_id = 0
        elif "ダ" in field:
            field = "ダート"
            field_id = 1
        else:
            field = ""
            field_id = 10
        field_id
        #距離
        kyori = race.find(class_="data_intro").find("dl").find("dd").find("p").find("diary_snap_cut").find("span").text.split("/")
        kyori = int_(kyori[0][2:].replace("m","").replace("\xa0","").replace("\ufffd",""))
        #天気
        weather = race.find(class_="data_intro").find("dl").find("dd").find("p").text
        if "晴" in weather:
            weather = "晴"
            weather_id = 0
        elif "曇" in weather:
            weather = "曇"
            weather_id = 1
        elif "小雨" in weather:
            weather = "小雨"
            weather_id = 2
        elif "雨" in weather:
            weather = "雨"
            weather_id = 3
        elif "小雪" in weather:
            weather = "小雪"
            weather_id = 4
        elif "雪" in weather:
            weather = "雪"
            weather_id = 5
        else:
            weather = ""
            weather_id = 10
        weather_id
        #馬場
        baba = race.find(class_="data_intro").find("dl").find("dd").find("p").find("diary_snap_cut").find("span").text
        if "良" in baba:
            baba = "良"
            baba_id = 0
        elif "稍重" in baba:
            baba = "稍重"
            baba_id = 1
        elif "重" in baba:
            baba = "重"
            baba_id = 2
        elif "不良" in baba:
            baba = "不良"
            baba_id = 3
        else:
            baba = ""
            baba_id = 10
        #発走時刻
        str_time = race.find(class_="data_intro").find("dl").find("dd").find("p").find("diary_snap_cut").find("span").text.replace("\xa0","").replace("\ufffd","").split("/")
        str_time = str_time[3][-5:]
        #回り
        mawari = race.find(class_="data_intro").find("dl").find("dd").find("p").text
        if "右" in mawari:
            mawari = "右"
            mawari_id = 0
        elif "左" in mawari:
            mawari =  "左"
            mawari_id = 1
        else:
            mawari = ""
            mawari_id = 10
        #頭数
        uma_table = race.find(class_="race_table_01 nk_tb_common").find_all("tr")
        tousuu = len(uma_table)-1

        race_info["Date"] = date
        race_info["Start_Time"] = str_time
        race_info["Place"] =  place
        race_info["Place_Id"] = place_id
        race_info["Race_Num"] = race_num
        race_info["Race_Id"] = self.id
        race_info["Class"] = class_txt
        race_info["Class_Id"] = class_id
        race_info["Tousuu"] = tousuu
        race_info["Field"] = field
        race_info["Field_Id"] = field_id
        race_info["Kyori"] = kyori
        race_info["Mawari"] = mawari
        race_info["Mawari_Id"] = mawari_id
        race_info["Baba"] = baba
        race_info["BaBa_Id"] = baba_id
        race_info["Weather"] = weather
        race_info["Weather_Id"] = weather_id

        return uma_table, race_info

    def get_detail(self, uma_list):
        """馬ごとの情報を取得

        Args:
            uma_list (list): 馬のリスト

        Returns:
            details (dict): 馬ごとの情報
        """
        details = {}

        #uma_list = uma_table[1]
        uma_info = uma_list.find_all("td")
        #着順
        rank = uma_info[0].text.replace("\xa0","").replace("\ufffd","")
        if "中" in rank or "取" in rank or "除" in rank:
            rank = ""
            time = ""
            delay = ""
            corner = ""
            f3 = ""
            tansho = ""
            ninki = ""

        else:
            rank = int_(rank)
            time = uma_info[7].text.split(":")
            time = float(time[0]) * 60 + float(time[1])
            if rank == 1:
                delay = "0"
            else:
                delay = uma_info[8].text.replace("\xa0","").replace("\ufffd","")
            corner = uma_info[10].text.replace("\xa0","").replace("\ufffd","")
            f3 = float_(uma_info[11].text.replace("\xa0","").replace("\ufffd",""))
            tansho = float(uma_info[12].text.replace("\xa0","").replace("\ufffd",""))
            ninki = int(uma_info[13].text.replace("\xa0","").replace("\ufffd",""))

        #枠
        waku = int_(uma_info[1].text.replace("\xa0","").replace("\ufffd",""))
        #馬番
        uma_num = int_(uma_info[2].text.replace("\xa0","").replace("\ufffd",""))
        #馬名
        name = uma_info[3].find("a").text.replace("\xa0","").replace("\ufffd","")
        #馬id
        uma_id = uma_info[3].find("a").get("href").split("/")
        uma_id = int_(uma_id[2])
        #性別
        sex_age = uma_info[4].text.replace("\xa0","").replace("\ufffd","")
        sex = sex_age[0]
        if "牡" in sex:
            sex_id = 0
        elif "牝" in sex:
            sex_id = 1
        elif "セ" in sex:
            sex_id =2
        else:
            sex_id = 10
        #年齢
        age = int_(sex_age[1:])
        #斤量
        jockey_weight = float(uma_info[5].text.replace("\xa0","").replace("\ufffd",""))
        #騎手
        jockey = uma_info[6].find("a").text.replace("\xa0","").replace("\ufffd","")
        #騎手id
        jockey_id = uma_info[6].find("a")
        if jockey_id is None:
            jockey_id = ""
        else:
            jockey_id = int_(jockey_id.get("href").split("/")[4])
        #タイム
        #着差
        #通過
        #上り
        #単勝
        #人気
        #馬体重
        #体重増減
        weight = uma_info[14].text.replace("(","").replace(")","").replace("\xa0","").replace("\ufffd","")
        if weight == "計不" or weight == "":
            weight_today = ""
            weight_change = ""
        else:    
            weight_today = int_(weight[:3])
            weight_change = int_(weight[3:])
        #調教師
        trainer = uma_info[18].find("a").text.replace("\xa0","").replace("\ufffd","").replace("\n","")
        #調教師id
        trainer_id = uma_info[18].find("a")
        if trainer_id is None:
            trainer_id = ""
        else:
            trainer_id = int_(trainer_id.get("href").split("/")[4])
        #馬主
        owner = uma_info[19].text.replace("\xa0","").replace("\ufffd","").replace("\n","")
        #馬主id
        owner_id = uma_info[19].find("a")
        if owner_id is None:
            owner_id = ""
        else:
            owner_id = int_(owner_id.get("href").split("/")[4])

        details["Rank"] = rank
        details["Waku"] = waku
        details["Number"] = uma_num
        details["Name"] = name
        details["Uma_Id"] = uma_id 
        details["Sex"] = sex
        details["Sex_Id"] = sex_id
        details["Age"] = age
        details["Jockey_Weight"] = jockey_weight
        details["Jockey"] = jockey
        details["Jockey_Id"] = jockey_id
        details["Time"] = time
        details["Delay"] = delay
        details["Ninki"] = ninki
        details["Tansho"] = tansho
        details["3F"] = f3
        details["Corner"] = corner 
        details["Weight"] = weight_today
        details["Weight_Change"] = weight_change
        details["Trainer"] = trainer
        details["Trainer_Id"] = trainer_id
        details["Owner"] = owner
        details["Owner_Id"] = owner_id

        return details

    def get_id(self, output_filename, columns=[]):
        """指定したidを取得

        Parameters
        ----------
        output_filename : str, int
            出力ファイル名
        columns : list
            idを取得する行名のリスト
        """
        if not columns:
            columns = ["Uma_Id", "Jockey_Id", "Trainer_Id", "Owner_Id"]

        for column in columns:
            # テキストファイルを出力するフォルダを生成
            output_dir = column.lower()
            if not os.path.exists(output_dir):
                os.mkdir(output_dir)
            # テキストファイルにidを書き込む
            file_path = "{}/{}/{}.txt".format(self.current_dir, output_dir, output_filename)
            id_ = self.race_data[column].drop_duplicates().values.tolist() # idの抽出
            obj = map(lambda x: str(x) + "\n", id_)
            # データの保存
            with open(file_path, "w", encoding="utf-8") as f:
                f.writelines(obj)

    def output(self, output_filename):
        """取得したデータを出力

        Parameters
        ----------
        output_filename : str, int
            出力ファイル名
        """
        self.race_data.Name = self.race_data.Name.str.strip() # 改行文字を削除
        self.race_data.Rank = self.race_data.Rank.astype('Int64', errors='ignore') # Rank列がなぜかfloatになるのでint型に変換
        super().output("{}_all_race".format(output_filename)) # 出力
    
# ---------------------------------------------------------------------------
# Payout_Crawler
# ---------------------------------------------------------------------------

class Payout_Crawler(Crawler):
    def __init__(self, sleep_time=1, if_exception="pass", input_dir="race_id", output_dir="payout_csv_data"):
        """払い戻し情報をスクレイピングするクラス

        Attributes:
        ----------
        sleep_time : int, default 1
            id毎に停止する時間
        if_exception : str, default "pass"
            例外が出た時の処理
        current_dir : str
            現在のディレクトリ
        input_dir : str
            idが保存されているフォルダ名
        output_dir : str
            csvファイルを出力するフォルダ名

        Notes
        -----
        if_exception = "pass"  -> 例外が出た時passしてそのrace_idを記録する
        if_exception = "raise" -> 例外が出た時エラーを出力
        """
        super().__init__(sleep_time, if_exception, input_dir, output_dir)

    def get_one_id_race_data(self, id=None) -> list:
        """id一つ分の払い戻し情報を取得する

        Parameters
        ----------
        id : int, default None
            レースid

        Returns
        -------
        data : list
            レース情報のリスト
        """
        if id is not None:
            self.id = id

        race_url = "https://db.netkeiba.com/race/" + self.id + "/"

        race_html =  requests.get(race_url)
        race_html.encoding = "EUC-JP"
        race = BeautifulSoup(race_html.text, 'html.parser')

        #レース名
        #race_class = race.find(class_="data_intro").find("h1").text

        #払い戻し
        payout_table = race.find(class_="pay_block").find_all("tr")
        details = {}
        details["Race_Id"] = self.id
        for po_list in payout_table:
            #po_list = payout_table[1]
            if "単勝" in po_list.text:
                tan = po_list.find_all("td")[1].text.replace(",","")
                tan = float(tan)/100
                details["Tansho"] = tan

            elif "複勝" in po_list.text:
                huku_num = list(po_list.find_all("td")[0].children)
                huku_odds = list(po_list.find_all("td")[1].children)
                huku_num = huku_num[0::2]
                huku_odds = huku_odds[0::2]
                for i in range(len(huku_num)):
                    details["Huku_Num" + str(i + 1)] = huku_num[i]
                    details["Huku_Odds" + str(i + 1)] = float(huku_odds[i].replace(",",""))/100

            elif "枠連" in po_list.text:
                wakuren = po_list.find_all("td")[1].text.replace(",","")
                wakuren = float(wakuren)/100
                details["Wakuren"] = wakuren

            elif "馬連" in po_list.text:
                umaren = po_list.find_all("td")[1].text.replace(",","")
                umaren = float(umaren)/100
                details["Umaren"] = umaren

            elif "ワイド" in po_list.text:
                wide_num = list(po_list.find_all("td")[0].children)
                wide_odds = list(po_list.find_all("td")[1].children)
                wide_num = wide_num[0::2]
                wide_odds = wide_odds[0::2]
                for i in range(len(wide_num)):
                    details["Wide_Num" + str(i + 1)] = wide_num[i]
                    details["Wide_Odds" + str(i + 1)] = float(wide_odds[i].replace(",",""))/100

            elif "馬単" in po_list.text:
                umatan = po_list.find_all("td")[1].text.replace(",","")
                umatan = float(umatan)/100
                details["Umatan"] = umatan

            elif "三連複" in po_list.text:
                sanrenpuku = po_list.find_all("td")[1].text.replace(",","")
                sanrenpuku = float(sanrenpuku)/100
                details["Sanrenpuku"] = sanrenpuku

            elif "三連単" in po_list.text:
                sanrentan = po_list.find_all("td")[1].text.replace(",","")
                sanrentan = float(sanrentan)/100
                details["Sanrentan"] = sanrentan
        details = [details]
        return details 

    def output(self, output_filename):
        """取得したデータを出力

        Parameters
        ----------
        race_id : int, default None
            レースid

        Returns
        -------
        data : list
            レース情報のリスト
        """
        super().output("{}_all_payout".format(output_filename)) # 出力

# ---------------------------------------------------------------------------
# Horse_Info_Crawler
# ---------------------------------------------------------------------------

class Horse_Info_Crawler(Crawler):
    def __init__(self, sleep_time=1, if_exception="pass", input_dir="uma_id", output_dir="umainfo_csv_data"):
        """馬情報をスクレイピングするクラス

        Attributes:
        ----------
        sleep_time : int, default 1
            id毎に停止する時間
        if_exception : str, default "pass"
            例外が出た時の処理
        current_dir : str
            現在のディレクトリ
        input_dir : str
            idが保存されているフォルダ名
        output_dir : str
            csvファイルを出力するフォルダ名

        Notes
        -----
        if_exception = "pass"  -> 例外が出た時passしてそのrace_idを記録する
        if_exception = "raise" -> 例外が出た時エラーを出力
        """
        super().__init__(sleep_time, if_exception, input_dir, output_dir)
    
    def get_one_id_race_data(self, id=None) -> list:
        """id一つ分の馬情報を取得

        Parameters
        ----------
        id : int, default None
            レースid

        Returns
        -------
        data : list
            レース情報のリスト
        """

        if id is not None:
            self.id = id

        horse_url = "https://db.netkeiba.com/horse/" + self.id + "/"

        horse_html =  requests.get(horse_url)
        horse_html.encoding = "EUC-JP"
        horse = BeautifulSoup(horse_html.text, 'html.parser')

        details = {}
        details["Uma_Id"] = self.id

        infos = horse.find(class_="db_prof_table").find_all("tr")
        blood = horse.find(class_="blood_table").find_all("td")
        # past_races = horse.find(class_="db_h_race_results nk_tb_common").find("tbody").find_all("tr")

        for info in infos:
            info_txt = info.text
            if "生年月日" in info_txt:
                birthday = info.find("td").text
                birthday = datetime.datetime.strptime(birthday, "%Y年%m月%d日")
                details["Birthday"] = birthday

            elif "調教師" in info_txt:
                trainer = info.find("td").text.replace("\n","")
                trainer_id = info.find("td").find("a")
                if trainer_id is None:
                    trainer_id = ""
                else:
                    trainer_id = int_(trainer_id.get("href").split("/")[2])
                details["Trainer"] = trainer
                details["Trainer_Id"] = trainer_id

            elif "馬主" in info_txt:
                owner = info.find("td").text.replace("\n","")
                owner_id = info.find("td").find("a")
                if owner_id is None:
                    owner_id = ""
                else:
                    owner_id = int_(owner_id.get("href").split("/")[2])
                details["Owner"] = owner
                details["Owner_Id"] = owner_id

            elif "生産者" in info_txt:
                breeder = info.find("td").text.replace("\n","")
                breeder_id = info.find("td").find("a")
                if breeder_id is None:
                    breeder_id = ""
                else:
                    breeder_id = int_(breeder_id.get("href").split("/")[2])
                details["Breeder"] = breeder
                details["Breeder_Id"] = breeder_id

            elif "産地" in info_txt:
                sanchi = info.find("td").text.replace("\n","")
                details["Sanchi"] = sanchi
            elif "通算成績" in info_txt:
                result_rate = info.find("td").text.replace("\n","").split("[")[0].replace("勝","").split("戦")
                if result_rate[0] == "0" or result_rate[1] == "0":
                    result_rate = 0
                else: 
                    result_rate = float(result_rate[1])/float(result_rate[0])
                result_detail = info.find("td").text.replace("\n","").replace("]","").split("[")[1]
                details["Result_Rate"] = result_rate
                details["Result_Detail"] = result_detail

        father = blood[0].text.replace("\n","")
        father_id = blood[0].find("a").get("href").split("/")[3]
        f_father = blood[1].text.replace("\n","")
        f_father_id = blood[1].find("a").get("href").split("/")[3]
        f_mother = blood[2].text.replace("\n","")
        f_mother_id = blood[2].find("a").get("href").split("/")[3]
        mother = blood[3].text.replace("\n","")
        mother_id = blood[3].find("a").get("href").split("/")[3]
        m_father = blood[4].text.replace("\n","")
        m_father_id = blood[4].find("a").get("href").split("/")[3]
        m_mother = blood[5].text.replace("\n","")
        m_mother_id = blood[5].find("a").get("href").split("/")[3]
        details["Father"] = father
        details["Father_id"] = father_id
        details["F_Father"] = f_father
        details["F_Father_Id"] = f_father_id
        details["F_Mother"] = f_mother
        details["F_Mother_Id"] = f_mother_id
        details["Mother"] = mother
        details["Mother"] = mother_id
        details["M_Father"] = m_father
        details["M_Father_Id"] = m_father_id
        details["M_Mother"] = m_mother
        details["M_Mother_Id"] = m_mother_id
        details = [details]
        return details 

# ---------------------------------------------------------------------------
# function(int_, get_id)
# ---------------------------------------------------------------------------

def int_(data):
    """str -> int型に変換する

    Parameters
    ----------
    data : str
        変換前のデータ

    Returns
    -------
    data : int
        変換後のデータ
    """
    # 文字列が含まれる場合空文字に置き換え
    data = re.sub(r"\D", "", data)
    # それでもエラーが出る場合は欠損値扱い
    try:
        data = int(data)
    except:
        data = ""
    return data

def float_(data):
    """str -> float型に変換する

    Parameters
    ----------
    data : str
        変換前のデータ

    Returns
    -------
    data : float
        変換後のデータ
    """
    if data == '':
        data = ""
    else:
        data = float(data)
    return data

def get_id(*years):
    """指定したidを取得

    Parameters
    ----------
    years : tuple
        ファイル名（拡張子不要）
    """
    current_dir_ = os.getcwd() # カレントディレクトリを取得
    columns = ["Uma_Id", "Jockey_Id", "Trainer_Id", "Owner_Id"]

    for year in years:
        # データを読み込む
        try:
            df = pd.read_pickle("{}/race_pickle_data/{}_all_race.pickle".format(current_dir_ , year))
        except:
            df = pd.read_csv("{}/race_csv_data/{}_all_race.csv".format(current_dir_ , year), encoding="shift-jis")

        for column in columns:
            # テキストファイルを出力するフォルダを生成
            output_dir = column.lower()
            if not os.path.exists(output_dir):
                os.mkdir(output_dir)
            # テキストファイルにidを書き込む
            file_path = "{}/{}/{}.txt".format(current_dir_, output_dir, year)
            id_ = df[column].drop_duplicates().values.tolist() # idの抽出
            obj = map(lambda x: str(x) + "\n", id_)
            # データの保存
            with open(file_path, "w", encoding="utf-8") as f:
                f.writelines(obj)

    # 処理を行った年を出力
    years = [str(year) for year in years]
    years = " ".join(years)
    print("\ryear : {}\n".format(years), end="")
            

if __name__ == '__main__':
    # crawler = Race_Crawler(sleep_time=0.5, if_exception="raise", get_id=False)
    # crawler = Payout_Crawler(sleep_time=0.5, if_exception="raise")
    crawler = Horse_Info_Crawler(sleep_time=1, if_exception="pass")
    crawler(2001)
    # get_id(2000, 2001, 2002, 2010, 2011, 2012, 2013)
    # レースid1つ分だけ取得
    # crawler.get_one_id_race_data(race_id)

__version__ = '2.0.3'