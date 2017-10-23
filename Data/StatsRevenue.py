# -*- coding: utf-8 -*-

from Utility.DB import DB
from ImExport import ImExport
import pandas as pd
import numpy as np




#
class StatsRevenue:

    def __init__(self):
        self.db = DB('localhost', 'stockresult', 'root', '0910@mysql')

    def compute_revenue(self, x):
        if x['isLong'] == 0:
            return x['InLastPrice'] - x['OutLastPrice']
        else:
            return x['OutLastPrice'] - x['InLastPrice']

    def compute_revenue_ML(self, x):
        if (x['MyOutput1'] == 0 and x['isLong'] == 0) or (x['MyOutput1'] == 1 and x['isLong'] == 1) :
            return x['OutLastPrice'] - x['InLastPrice']
        else:
            return x['InLastPrice'] - x['OutLastPrice']

    def save_revenue_mysql(self, imex, fromtable, chunksize, totable, filedlist, isML, MLtags):

        trade_num = self.db.select(fromtable, "count(*)")[0]['count(*)'] / 2
        start = 0
        offset = 100000

        while True:
            sql1 = "select * from " + fromtable + " where isOpen=1 limit " + str(start) + "," + str(start+offset)
            df1 = pd.read_sql(sql1, self.db.conn)
            df1.rename(columns={'Times': 'InTimes', 'LastPrice': 'InLastPrice'}, inplace=True)

            sql2 = "select * from " + fromtable + " where isOpen=0 limit " + str(start) + "," + str(start+offset)
            df2 = pd.read_sql(sql2, self.db.conn)
            df2.rename(columns={'Times': 'OutTimes', 'LastPrice': 'OutLastPrice'}, inplace=True)


            df2 = df2.loc[:, ["OutTimes", "OutLastPrice","isOpen"]]
            df3 = pd.concat([df1, df2], axis=1, join='inner')


            if isML:
                for tag in MLtags:
                    df3['Revenue_'+tag] = map(lambda x, y, z, t: z - y if x + t == 1
                                              else y - z, df3['isLong'], df3['OutLastPrice'], df3['InLastPrice'], df3[tag])
            else:
                df3['Revenue'] = map(lambda x, y, z: y - z if x > 0
                else z - y, df3['isLong'], df3['OutLastPrice'], df3['InLastPrice'])


            #print df3.ix[[1660,1661,1662], ['InTimes', 'OutTimes', 'InLastPrice','OutLastPrice','isLong', 'Revenue_pro_model', MLtags[0]]]

            imex.save_df_mysql(df3, totable, False)
            start += offset
            if start > trade_num:
                break
            print "---------------------------"

    def stats_revenue(self, from_table, condition, Revenue):

        paras = ""
        for con in condition:
            paras += con + ", "
        paras = paras[:-2]

        sql0 = "select " + paras + ", count(*), sum(" + Revenue + "), avg(" + Revenue + ") from " + from_table

        sql1 = "select " + paras + ", count(*), sum(" + Revenue + "), avg(" + Revenue + "), max(" + Revenue \
               + "), min(" + Revenue + ") from " + from_table + " group by " + paras
        df1 = pd.read_sql(sql1, self.db.conn)
        df1.rename(columns={'count(*)': 'tradeNum', 'sum(' + Revenue + ')': 'total_revenue',
                            'avg(' + Revenue + ')': 'avg_reveune', 'max(' + Revenue + ')': 'max_reveune',
                            'min(' + Revenue + ')': 'min_reveune'}, inplace=True)
        print sql1
        print df1

        sql2 = sql0 + " where " + Revenue + ">0 " + " group by " + paras
        df2 = pd.read_sql(sql2, self.db.conn)
        df2.rename(columns={'count(*)': 'winNum', 'sum(' + Revenue + ')': 'total_revenue(win)',
                            'avg(' + Revenue + ')': 'avg_revenue(win)'}, inplace=True)

        sql3 = sql0 + " where " + Revenue + "<0 " + " group by " + paras
        df3 = pd.read_sql(sql3, self.db.conn)
        df3.rename(columns={'count(*)': 'loseNum', 'sum(' + Revenue + ')': 'total_revenue(lose)',
                            'avg(' + Revenue + ')': 'avg_revenue(lose)'}, inplace=True)

        df = pd.merge(df1, df2, how='left', on=condition)
        df = pd.merge(df, df3, how='left', on=condition)
        df['winPercent'] = df['winNum'] / df['tradeNum']
        df.fillna(0)

        return df

if __name__ == '__main__':
    S = StatsRevenue()


    db = DB('localhost', 'stockresult', 'root', '0910@mysql')
    imex = ImExport(db)
    MLtags = ['pro_model', 'acc_model', 'eff_model']
    #S.save_revenue_mysql(imex, "20171019expect", 100000, "revenue20171023", "", False, MLtags)


    Reveunes = ['Revenue']
    for Reveune in Reveunes:
        print Reveune

        condition = ["MID(InTimes,1,6)", "ComputeLantency", "IntervalNum", "MuUpper"]
        tables = ["revenue20171023"]
        for table in tables:
            df = S.stats_revenue(table, condition, Reveune)
            IM = ImExport(S.db)
            IM.save_df_csv(df, "/Users/songxue/Desktop/", "stats_" + table + ".csv")


