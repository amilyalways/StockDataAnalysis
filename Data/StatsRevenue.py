# -*- coding: utf-8 -*-

from Utility.DB import DB
from ImExport import ImExport
import pandas as pd
import numpy as np

#
class StatsRevenue:

    def __init__(self):
        self.db = DB('localhost', 'stockresult','root','0910@mysql')
    '''
    def compute_revenue(self, price1, price2, isLong):

        if isLong == 0:
            t = -1
        else:
            t = 1
        return (price2 - price1) * t
    '''
    def compute_revenue(self, x):
        if x['isLong'] == 0:
            return x['InLastPrice'] - x['OutLastPrice']
        else:
            return x['OutLastPrice'] - x['InLastPrice']

    def save_revenue_mysql(self, fromtable, chunksize, totable, filedlist):

        trade_num = self.db.select(fromtable, "count(*)")[0]['count(*)'] / 2
        start = 0
        offset = 10000

        while True:
            sql1 = "select * from " + fromtable + " where isOpen=1 limit " + str(start) + "," + str(start+offset)
            df1 = pd.read_sql(sql1, self.db.conn)
            df1.rename(columns={'Times': 'InTimes', 'LastPrice': 'InLastPrice'}, inplace=True)

            sql2 = "select * from " + fromtable + " where isOpen=0 limit " + str(start) + "," + str(start+offset)
            df2 = pd.read_sql(sql2, self.db.conn)
            df2.rename(columns={'Times': 'OutTimes', 'LastPrice': 'OutLastPrice'}, inplace=True)


            df2 = df2.loc[:, ["OutTimes", "OutLastPrice","isOpen"]]
            df3 = pd.concat([df1, df2], axis=1, join='inner')


            df3['Revenue'] = map(lambda x, y, z: y-z if x >0
                                 else z-y, df3['isLong'], df3['OutLastPrice'], df3['InLastPrice'])
            print df3.ix[[0,1,2], ['InTimes', 'OutTimes', 'InLastPrice','OutLastPrice','isLong', 'Revenue']]

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

        df2['winPercent'] = df2['winNum'] / df1['tradeNum']

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
    tables = ["revenue20170921"]
    for table in tables:
        df = S.stats_revenue(table, condition)
        IM = ImExport(S.db)
        IM.save_df_csv(df, "/home/emily/桌面/stockResult/stats20170921/", "stats_month_" + table +".csv")





    #S.save_revenue_mysql("tradeinfos20170914", 10000, "", "")

