import pandas as pd
import numpy as np
import sqlalchemy as sa
import warnings

warnings.simplefilter('ignore')

# SQLの環境構築
# SQLの起動
# データベースの作成

# インポートが必要なライブラリ
# pip3 install sqlalchemy
# pip3 install ipython-sql 
# pip3 install pymysql

class PySQL:
    def __init__(self, password="srs1123", database_name="horse_racing", if_exists='replace', encoding="shift-jis"):
        """csvファイルのデータをデータベースへ保存する

        Parameters
        ----------
        password : str
            MySQLのパスワード（SQL環境構築時に設定した値）
        schema_name : str
        """
        self.database_name = database_name
        self.url = "mysql+pymysql://root:"+password+"@localhost:3306/"+self.database_name+"?charset=utf8mb4"
        self.engine = sa.create_engine(self.url, echo=False)
        self.if_exists = if_exists
        self.encoding = encoding
    
    def __call__(self, start_year, end_year):
        years = np.arange(start_year, end_year+1, 1)
        for year in years:
            self.to_sql(year)
            print("\r{}".format(year), end='')

    def to_sql(self, year=None, df_race=None, tbl_name=None):
        """csvファイルのデータをデータベースへ保存する

        Parameters
        ----------
        year : レース開催日
            レース開催日
        if_exists : str
            テーブルが既にある場合の処理
        tbl_name : str
            データベースに保存する時のテーブル名
        """

        # csvファイルの読み込み
        if year is not None:
            file_name = "{}_all_race".format(str(year))
            df_race = pd.read_csv("race_csv_data/{}.csv".format(file_name), encoding=self.encoding).loc[:, "Rank":]

        # データフレームを保存
        if tbl_name is None:
            tbl_name = file_name
        df_race.to_sql(con=self.engine,name=tbl_name, schema=self.database_name, if_exists=self.if_exists, index=False)
    
    def read_sql(self, year=None, query=None):
        """データベースをデータフレーム形式で読み込む

        Parameters
        ----------
        query : str
            SQLのクエリ

        Returns
        -------
        df : pandas.DataFrame
            データフレーム
        """
        if year is not None:
            query = "SELECT * FROM " + str(year) + "_all_race"
        df = pd.read_sql(query, con = self.engine)
        return df

if __name__ == '__main__':
    password = "各自で設定" # MySQLで設定したパスワード
    database_name = "horse_racing" # データベース名（データベースを作る必要あり）

    pysql = PySQL(password=password, database_name=database_name)
    pysql(2014, 2021)