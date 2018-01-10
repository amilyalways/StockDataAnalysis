# -*- coding: utf-8 -*-
import pandas as pd
from Utility.DB import DB
from Utility.TimeTransfer import TimeTransfer
from matplotlib import pyplot as plt
import os
import numpy as np
from sqlalchemy import create_engine


class ImExport:

    def __init__(self, db):
        self.db = db


    def excel_to_df(self, path, filename, sheetname):
        df = pd.read_excel(path+filename, sheetname=sheetname)
        return df

    def mysqlToCSV(self, sql, chunksize, path, resultTableName):
        result = pd.read_sql(sql, self.db.conn, chunksize=chunksize)
        header = True
        for re in result:
            re.to_csv(path+resultTableName, index=False, header=header, mode='a')
            header = False

    def save_df_csv(self, df, path, filename):
        if not os.path.exists(path):
            os.makedirs(path)
        df.to_csv(path + filename)

    def save_df_mysql(self, df, tablename, index):
        engine = create_engine('mysql+pymysql://root:0910@mysql@localhost/stockresult', echo=False)
        df.to_sql(tablename, engine, if_exists='append', index=index)

if __name__ == '__main__':
    db = DB('localhost', 'stockresult', 'root', '0910@mysql')
    imex = ImExport(db)
    tablenames = ["revenue20180110_MLdown0.2_1.0_notune", "revenue20180110_MLdown0.4_1.8_notune", "revenue20180110_MLdown0.6_2.0_notune", "revenue20180110_MLdown0.8_2.4_notune"]
    #filenames = ["price_with_trade--0.2-1.0.csv", "price_with_trade--0.4-1.8.csv", "price_with_trade--0.6-2.0.csv", "price_with_trade--0.8-2.4.csv"]
    filenames = ["price_with_trade-down-0.2--1.0.csv", "price_with_trade-down-0.4--1.8.csv", "price_with_trade-down-0.6--2.0.csv", "price_with_trade-down-0.8--2.4.csv"]
    path = "/home/emily/下载/data20180110/"
    l = len(tablenames)


    filenames1 = ["rst-full-year-lr-up-0.0004-0.csv", "rst-full-year-lr-down-0.0004-0.csv", "rst-full-year-up-0.0004.csv",
                 "rst-full-year-down-0.0004.csv", "rst-full-year-up-0.0008.csv", "rst-full-year-down-0.0008.csv",]

    i = 0
    for filename in filenames1:
        df1 = pd.read_csv(path + filename, header=None, names=[filename[14:-4]])
        if i > 0:
            df3 = pd.concat([df3, df1], axis=1, join='inner')
        else:
            df3 = df1
        i += 1

    for i in range(0, l):
        #sql = "select * from " + tablenames[i]
        # imex.mysqlToCSV(sql, 10000, path, filename)
        df = pd.read_csv(path + filenames[i])
        df.rename(columns={'Times': 'InTimes'}, inplace=True)
        tt = TimeTransfer()
        df['OutTimes'] = map(lambda x, y: tt.long_to_time(tt.time_to_long(x) + 3 * y),
                             df['InTimes'], df['HoldTime'])
        cols = list(df)
        new_cols = []
        for col in cols:
            if col == "InTimes":
                new_cols.append(col)
                new_cols.append("OutTimes")
                new_cols.append("HoldTime")
            elif col == "LastPrice":
                new_cols.append(col)
                new_cols.append("AskPrice")
                new_cols.append("BidPrice")
                new_cols.append("RealProfitF")
                new_cols.append("Sign")
                new_cols.append("RealProfit")
                new_cols.append("RealProfitS")

        for col in cols:
            if col not in new_cols:
                new_cols.append(col)



        df = df[new_cols]
        df = pd.concat([df, df3], axis=1, join='inner')
        print df[:10]

        imex.save_df_mysql(df, tablenames[i], False)
        print "finish one write "





    '''
    Mus = np.array(np.arange(0.0001, 0.0005, 0.0001))
    Mu = 0.0004
    OutMuUpper = Mu
    for InMuUpper in Mus:
        sql = "select * from " + tablename + " where ComputeLantency=5 and IntervalNum=6 and InMuUpper=" + \
              str(InMuUpper) + " and " + "OutMuUpper=" + str(OutMuUpper)
        print sql
        imex.mysqlToCSV(sql, 100000, "/home/emily/桌面/stockResult/", "tradeinfos20170928.csv")
    InMuUpper = Mu
    for OutMuUpper in Mus[:-1]:
        sql = "select * from " + tablename + " where ComputeLantency=5 and IntervalNum=6 and InMuUpper=" + \
              str(InMuUpper) + " and " + "OutMuUpper=" + str(OutMuUpper)
        print sql
        imex.mysqlToCSV(sql, 100000, "/home/emily/桌面/stockResult/", "tradeinfos20170928.csv")
    '''
    '''
    print "total   ---------------"
    sql = "select Revenue from revenue20170914 where Times like '201306%'"
    df = pd.read_sql(sql, db.conn)
    print df.describe()
    print df.quantile(0.9)
    plt.rc('figure', figsize=(60,35))
    fig = plt.figure()
    ax1 = fig.add_subplot(3, 1, 1)
    df['Revenue'].plot(ax=ax1, style='--')
    print "Win      ---------------"
    sql = "select Revenue from revenue20170914 where Times like '201306%' and Revenue>0"
    df = pd.read_sql(sql, db.conn)
    print df.describe()
    print df.quantile(0.9)
    ax2 = fig.add_subplot(3, 1, 2)
    df['Revenue'].plot(ax=ax2, style='--')
    print "Lose     ---------------"
    sql = "select Revenue from revenue20170914 where Times like '201306%' and Revenue<0"
    df = pd.read_sql(sql, db.conn)
    print df.describe()
    print df.quantile(0.08)
    ax3 = fig.add_subplot(3, 1, 3)
    df['Revenue'].plot(ax=ax3, style='--')
    plt.savefig("/home/emily/桌面/m.png", bbox_inches='tight')
    '''
    # imex.mysqlToCSV("select * from t", 1000, "/Users/songxue/Desktop/", "t.csv")
    #print imex.excel_to_df("/home/emily/桌面/", "abc.xlsx", "Sheet1")

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
