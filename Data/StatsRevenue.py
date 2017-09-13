# -*- coding: utf-8 -*-

from Utility.DB import DB
from ImExport import ImExport
import pandas as pd


class StatsRevenue:

    def __init__(self):
        self.db = DB('localhost', 'stockresult','root','0910@mysql')

    def compute_revenue(self, price1, price2, isLong):

        if isLong == 0:
            t = -1
        else:
            t = 1
        return (price2 - price1) * t

    def save_revenue_mysql(self, fromtable, chunksize, totable, filedlist):

        trade_num = self.db.select(fromtable, "count(*)")[0]['count(*)'] / 2
        start = 0
        offset = 10000

        while True:
            sql1 = "select * from " + fromtable + " where isOpen=1 limit " + str(start) + "," + str(start+offset)
            df1 = pd.read_sql(sql1, self.db.conn)
            sql2 = "select * from " + fromtable + " where isOpen=0 limit " + str(start) + "," + str(start+offset)
            df2 = pd.read_sql(sql2, self.db.conn)

            print "df1: "
            print df1[:3]
            print "df2: "
            print df2[:3]
            print "append "

            df2 = df2.loc[:, ["Times", "LastPrice","isOpen"]]
            df3 = pd.concat([df1, df2], axis=1, join='inner')
            print df3[:3]

            start += offset
            if start > trade_num:
                break
            print "---------------------------"

    def stats_revenue(self, from_table, condition):

        paras = ""
        for con in condition:
            paras += con + ", "
        paras = paras[:-2]

        sql0 = "select " + paras + ", count(*), sum(Revenue), avg(Revenue) from " + from_table
        sql1 = sql0 + " group by " + paras
        df1 = pd.read_sql(sql1, self.db.conn)
        df1.rename(columns={'count(*)': 'tradeNum', 'sum(Revenue)': 'total_revenue',
                            'avg(Revenue)': 'avg_revenue'}, inplace=True)
        print sql1
        print df1

        sql2 = sql0 + " where Revenue>0 " + " group by " + paras
        df2 = pd.read_sql(sql2, self.db.conn)
        df2.rename(columns={'count(*)': 'winNum', 'sum(Revenue)': 'total_revenue(win)',
                            'avg(Revenue)': 'avg_revenue(win)'}, inplace=True)

        sql3 = sql0 + " where Revenue<0 " + " group by " + paras
        df3 = pd.read_sql(sql3, self.db.conn)
        df3.rename(columns={'count(*)': 'loseNum', 'sum(Revenue)': 'total_revenue(lose)',
                            'avg(Revenue)': 'avg_revenue(lose)'}, inplace=True)

        df = pd.merge(df1, df2, how='left', on=condition)
        df = pd.merge(df, df3, how='left', on=condition)

        return df


if __name__ == '__main__':
    S = StatsRevenue()

    condition = ["MID(Times,1,6)", "ComputeLantency", "IntervalNum", "InMuUpper", "lnLastPriceThreshold"]
    tables = ["revenue20170913", "revenue_cut20170913", "revenue_anti20170913"]
    for table in tables:
        df = S.stats_revenue(table, condition)
        IM = ImExport(S.db)
        IM.save_df_csv(df, "/home/emily/桌面/stockResult/stats20170913/", "stats_" + table +".csv")


    #S.save_revenue_mysql("tradeinfos20170426", 10000, "", "")

