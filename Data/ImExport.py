# -*- coding: utf-8 -*-
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

    def save_df_mysql(self, df, tablename, index, index_label):
        df.to_sql(tablename, self.db.conn, if_exists='append', index=index, index_label=index_label)

if __name__ == '__main__':
    db = DB('localhost', 'stockresult', 'root', '0910@mysql')
    imex = ImExport(db)
    # imex.mysqlToCSV("select * from t", 1000, "/Users/songxue/Desktop/", "t.csv")
    sql = "select * from data201306 limit 10"
    df = pd.read_sql(sql, db.conn)
    print df
    imex.save_df_mysql(df, "mm", True, "ID")
    '''
    #寻找满足不同tradeNum条件的,各个参数下最小的MuBound对应的记录
    tradeNumboundList = [1000, 2000, 3000]
    sql_set = "set sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'"
    db.cur.execute(sql_set)
    for tradeNumbound in tradeNumboundList:
        print "start to create m"
        sqlm = "create view m(TimeRang, ComputeLantency, IntervalNum, lnLastPriceThreshold, MuBound, tradeNum, avgHoldTime) as " \
               "select * from mubound20170909 where tradeNum>" + str(tradeNumbound) + " order by ComputeLantency,IntervalNum,lnLastPriceThreshold, MuBound desc"
        print sqlm
        db.cur.execute(sqlm)
        db.conn.commit()

        print "start to create t"
        sqlt = "create view t(TimeRang, ComputeLantency, IntervalNum, lnLastPriceThreshold, MuBound) as " \
               "select TimeRang, ComputeLantency, IntervalNum, lnLastPriceThreshold, max(MuBound) " \
               "from m group by ComputeLantency, IntervalNum, lnLastPriceThreshold"
        print sqlt
        db.cur.execute(sqlt)
        db.conn.commit()

        print "start to join two table"
        sql1 = "select t.*, m.tradeNum, m.avgHoldTime " \
               "from t inner join m on t.ComputeLantency=m.ComputeLantency and t.IntervalNum=m.IntervalNum " \
               "and t.lnLastPriceThreshold=m.lnLastPriceThreshold and t.MuBound=m.MuBound"
        print sql1
        imex.mysqlToCSV(sql1, 10000, "/home/emily/桌面/", "MuBoundTradeNum" + str(tradeNumbound) + ".csv")

        print "start to drop m,t"
        sql2 = "drop view m,t "
        db.cur.execute(sql2)
        db.conn.commit()
        '''
