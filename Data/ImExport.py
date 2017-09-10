import pandas as pd
from Utility.DB import DB


class ImExport:

    def __init__(self, db):
        self.db = db

    def mysqlToCSV(self, sql, chunksize, path, resultTableName):
        result = pd.read_sql(sql, self.db.conn, chunksize=chunksize)
        header = True
        for re in result:
            re.to_csv(path+resultTableName, index=False, header=header, mode='a')
            header = False

    def save_df_csv(self, df, path, filename):
        df.to_csv(path + filename)

    def save_df_mysql(self):
        pass




db = DB('localhost', 'stockresult','root','0910@mysql')
imex = ImExport(db)
imex.mysqlToCSV("select * from t", 1000, "/Users/songxue/Desktop/", "t.csv")

