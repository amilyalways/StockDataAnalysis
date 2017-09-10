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

        #self.db.create_table(fromtable, filedlist, isDrop=True)

        sql1 = "select * from " + fromtable + " where isOpen=1"
        df1s = pd.read_sql(sql1, self.db.conn, chunksize=chunksize)
        sql2 = "select * from " + fromtable + " where isOpen=0"
        df2s = pd.read_sql(sql2, self.db.conn, chunksize=chunksize)

        for df1, df2 in df1s, df2s:
            df1.append(df2)
            print df1




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
    '''
    condition = ["MID(Times,1,6)", "ComputeLantency", "IntervalNum", "InMuUpper", "lnLastPriceThreshold"]
    df = S.stats_revenue("revenue20170901", condition)
    IM = ImExport(S.db)
    IM.save_df_csv(df, "/Users/songxue/Desktop/", "x.csv")
    '''
    S.save_revenue_mysql("170807readcsv ", 10000, "", "")

