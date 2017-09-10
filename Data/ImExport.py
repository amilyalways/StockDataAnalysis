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

if __name__ == '__main__':
    db = DB('localhost', 'stockresult', 'root', '0910@mysql')
    imex = ImExport(db)
    # imex.mysqlToCSV("select * from t", 1000, "/Users/songxue/Desktop/", "t.csv")

    tradeNumboundList = [1000, 2000, 3000]
    sql_set = "set sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'"
    db.cur.execute(sql_set)
    for tradeNumbound in tradeNumboundList:
        sql1 = "select a.* from mubound20170909 as a where a.tradeNum >" + str(tradeNumbound) + " and " \
                                                                                                "MuBound = (select max(MuBound) from mubound20170909 " \
                                                                                                "where a.ComputeLantency = ComputeLantency and a.IntervalNum=IntervalNum and a.lnLastPriceThreshold=lnLastPriceThreshold)"
        sql = "select * from (select * from mubound20170909 " \
              "where tradeNum>" + str(tradeNumbound) + " order by MuBound desc)" \
                                                       " as a group by a.ComputeLantency, a.IntervalNum, a.lnLastPriceThreshold";
        imex.mysqlToCSV(sql1, 10000, "/home/emily/桌面/", "MuBoundTradeNum" + str(tradeNumbound) + ".csv")
