# -*- coding: utf-8 -*-

from Utility.DB import DB
from ImExport import ImExport
from Utility.TimeTransfer import TimeTransfer
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import os


class StatsRevenue:

    def __init__(self):
        self.db = DB('localhost', 'stockresult', 'root', '0910@mysql')

    def save_revenue_mysql(self, imex, fromtable, chunksize, totable, filedlist, isML, MLtags):

        trade_num = imex.db.select(fromtable, "count(*)")[0]['count(*)'] / 2
        print "tradenum: " + str(trade_num)
        start = 0
        offset = 500000

        while True:
            sql1 = "select * from " + fromtable + " where isOpen=1 limit " + str(start) + "," + str(offset)
            print sql1
            df1 = pd.read_sql(sql1, imex.db.conn)
            df1.rename(columns={'Times': 'InTimes', 'LastPrice': 'InLastPrice', 'mu':'v_InMu'}, inplace=True)
            #df1.rename(columns={'Times': 'InTimes', 'LastPrice': 'InLastPrice', 'Expected': 'InExpected', 'A': 'InA'}, inplace=True)
            print "df1 len: " + str(len(df1)) #注意如果再次进入这个循环,df1没有被覆盖,而是叠加在原来的结果上了...
            sql2 = "select * from " + fromtable + " where isOpen=0 limit " + str(start) + "," + str(offset)
            print sql2
            df2 = pd.read_sql(sql2, imex.db.conn)
            df2.rename(columns={'Times': 'OutTimes', 'LastPrice': 'OutLastPrice', 'mu': 'v_OutMu', 'inmu':'OutType'}, inplace=True)
            #df2.rename(columns={'Times': 'OutTimes', 'LastPrice': 'OutLastPrice', 'Expected': 'OutExpected', 'A': 'OutA',
             #                   'isMaxtime': 'isMaxTimeOut'}, inplace=True)
            print "df2 len: " + str(len(df2))

            df2 = df2.loc[:, ["OutTimes", "OutLastPrice", "isOpen", "v_OutMu", "OutType"]]
            #df2 = df2.loc[:, ["OutExpected", "OutA", "OutTimes", "OutLastPrice", "isOpen", "isMaxTimeOut"]]
            df3 = pd.concat([df1, df2], axis=1, join='inner')
            print "df3 len: " + str(len(df3))
            #df3['cutValue'] = df3['cutValue']/5


            if isML:
                for tag in MLtags:
                    df3['Revenue_'+tag] = map(lambda x, y, z, t: z - y if x + t == 1
                                              else y - z, df3['isLong'], df3['OutLastPrice'], df3['InLastPrice'], df3[tag])
            else:
                df3['Revenue'] = map(lambda x, y, z: y - z if x > 0
                else z - y, df3['isLong'], df3['OutLastPrice'], df3['InLastPrice'])


            #print df3.ix[[1660,1661,1662], ['InTimes', 'OutTimes', 'InLastPrice','OutLastPrice','isLong', 'Revenue_pro_model', MLtags[0]]]
            cols = list(df3)
            new_cols = []
            for col in cols:
                if col == "InTimes":
                    new_cols.append("InTimes")
                    new_cols.append("OutTimes")
                elif col == "InLastPrice":
                    new_cols.append("InLastPrice")
                    new_cols.append("OutLastPrice")
                    new_cols.append("Revenue")
                    #new_cols.append("isMaxTimeOut")
                elif col == "v_InMu":
                    new_cols.append("v_InMu")
                    new_cols.append("v_OutMu")
                    new_cols.append("OutType")

                    #new_cols.append("outIsMaxTime")
            for col in cols:
                if col not in new_cols:
                    new_cols.append(col)
            new_cols.remove("inmu")



            df3 = df3[new_cols]
            print "start to write db"

            imex.save_df_mysql(df3, totable, False)

            print start
            start += offset
            if start > trade_num:
                break
            print "---------------------------"

    def stats_revenue(self, from_table, condition, Revenue):

        paras = ""
        for con in condition:
            paras += con + ", "
        paras = paras[:-2]


        conditionAdd =  " InTimes not like '20130625%' and InTimes not like '20130624%' and InTimes not like '20130613%' "
        conditionAdd = ""


        sql0 = "select " + paras + ", count(*),  sum(" + Revenue + "), avg(" + Revenue + ") from " \
               + from_table
        sql1 = "select " + paras + ", count(*), count(distinct mid(InTimes,1,8)), sum(" + Revenue + "), avg(" + Revenue + "), max(" + Revenue \
               + "), min(" + Revenue + ") from " + from_table + " group by " + paras
        df1 = pd.read_sql(sql1, self.db.conn)
        df1.rename(columns={'count(*)': 'tradeNum', 'count(distinct mid(InTimes,1,8))': 'tradeDayNum',
                            'sum(' + Revenue + ')': 'total_revenue',
                            'avg(' + Revenue + ')': 'avg_reveune', 'max(' + Revenue + ')': 'max_reveune',
                            'min(' + Revenue + ')': 'min_reveune'}, inplace=True)
        df1['avgDayTradeNum'] = map(lambda x, y: x / y if y > 0 else 0, df1['tradeNum'], df1['tradeDayNum'])
        print sql1
        print df1

        sql2 = sql0 + " where " + Revenue + ">0 " + " group by " + paras
        print sql2
        df2 = pd.read_sql(sql2, self.db.conn)
        df2.rename(columns={'count(*)': 'winNum', 'sum(' + Revenue + ')': 'total_revenue(win)',
                            'avg(' + Revenue + ')': 'avg_revenue(win)'}, inplace=True)

        sql3 = sql0 + " where " + Revenue + "<0 " + " group by " + paras
        print sql3
        df3 = pd.read_sql(sql3, self.db.conn)
        df3.rename(columns={'count(*)': 'loseNum', 'sum(' + Revenue + ')': 'total_revenue(lose)',
                            'avg(' + Revenue + ')': 'avg_revenue(lose)'}, inplace=True)

        df = pd.merge(df1, df2, how='left', on=condition)
        df = pd.merge(df, df3, how='left', on=condition)
        df['winPercent'] = df['winNum'] / df['tradeNum']

        df.fillna(0, inplace=True)

        return df

    #统计在亏损的交易中，最大的可能盈利值
    def stats_maxWin(self, trade_table, to_table):
        sql1 = " select * from " + trade_table
        df1 = pd.read_sql(sql1, self.db.conn)

        tt = TimeTransfer()

        l = len(df1)
        print l

        for i in range(0, l):

            start = tt.time_to_long(df1.loc[i, "InTimes"])
            end = tt.time_to_long(df1.loc[i, "OutTimes"])
            trend_table = "Ldata" + df1.loc[i, "InTimes"][:6]
            print i
            print trend_table

            isLong = df1.loc[i, "isLong"]

            if isLong:
                content = "desc"
            else:
                content = ""

            sql2 = " select * from " + trend_table \
                   + " where L_time>" + str(start) + " and L_time<=" + str(end) \
                   + " order by LastPrice " + content + ",L_time limit 1"
            df2 = pd.read_sql(sql2, self.db.conn)
            if(i == 0):
                df3 = df2
            else:
                df3 = pd.concat([df3, df2])


        #df3 = df3.loc[:, ["Times", "LastPrice"]]
        df3['id'] = [x for x in range(0, len(df3))]
        df3 = df3.set_index('id')


        df4 = pd.concat([df1, df3], axis=1, join_axes=[df1.index])
        df4.rename(columns={'Times': 'MiddleTimes', 'LastPrice': 'MiddleLastPrice'}, inplace=True)


        df4['MiddleRevenue'] = map(lambda x, y, z: y - z if x > 0
        else z - y, df4['isLong'], df4['MiddleLastPrice'], df4['InLastPrice'])

        df4['HoldTime'] = map(lambda x, y: tt.time_to_long(x) - tt.time_to_long(y),
                              df4['OutTimes'], df4['InTimes'])


        df4['LTime1'] = map(lambda x: tt.time_to_long(x), df4['InTimes'])
        df4['Time1'] = df4['L_time'] - df4['LTime1']

        cols = list(df4)
        new_cols = []
        for col in cols[:-7]:
            new_cols.append(col)
            if col == "InTimes":
                new_cols.append("MiddleTimes")
            elif col == "OutTimes":
                new_cols.append("HoldTime")
                new_cols.append("Time1")
            elif col == "InLastPrice":
                new_cols.append("MiddleLastPrice")
            elif col == "OutLastPrice":
                new_cols.append("MiddleRevenue")


        df4 = df4[new_cols]
        print df4

        imex.save_df_mysql(df4, to_table, False)

    #查看数据分布情况
    def distribution(self, sql, isPrint, isSave, imex, path, filename):
        df = pd.read_sql(sql, self.db.conn)
        df1 = df.describe()
        if isPrint:
            print df1

        if isSave:
            imex.save_df_csv(df1, path, filename)
        return df

    #可视化数据分布情况
    def visualization(self, df, plt, kind, isShow, isSave, title, xlabel, ylabel,
                      path, figname, **config):
        df.plot(kind=kind, figsize=(60, 35), linewidth=5)

        if isShow:
            plt.show()

        if isSave:
            plt.xlabel(xlabel)
            plt.ylabel(ylabel)
            plt.title(title, fontsize=60)
            plt.xticks(fontsize=45)
            plt.yticks(fontsize=45)

            plt.savefig(path + figname, **config)

    #关于最优平仓时间相关的利润和持仓时间上的统计和可视化
    def bestMiddleTimeRevenue(self, tablename, imex, path, filenames, plt, kind, titles):

        contents = [["Revenue", "MiddleRevenue"], ["HoldTime", "Time1"]]
        j = 0
        for content in contents:
            sql1 = "select " + content[0] + "," + content[1] + " from " + tablename + " where HoldTime<2000 "
            sql2 = sql1 + " and Revenue<=0 and MiddleRevenue>0"
            sql3 = sql1 + " and Revenue>0 and MiddleRevenue>0"
            sqls = [sql1, sql2, sql3]
            renames = [
                {
                    content[0]: "L" + content[0],
                    content[1]: "L" + content[1],
                },
                {
                    content[0]: "W" + content[0],
                    content[1]: "W" + content[1],
                }
            ]

            dfRevenue = []
            for i in range(0, 3):
                df0 = pd.read_sql(sqls[i], self.db.conn)
                title = titles[j] + "_" + str(i)
                figname = title + ".png"
                self.visualization(df0, plt, kind, False, True, title, "", "", path, figname)
                df0["diff" + str(i)] = df0[content[1]] - df0[content[0]]
                df1 = df0.describe()
                if i > 0:
                    df1.rename(columns=renames[i - 1], inplace=True)
                dfRevenue.append(df1)
            df = pd.concat(dfRevenue, axis=1)

            print df
            imex.save_df_csv(df, path, filenames[j])
            j += 1

    def seekLastPrice(self, tablename, Ltime):
        sql = "select * from " + tablename + " where L_time=" + str(Ltime)
        df1 = pd.read_sql(sql, self.db.conn)
        if len(df1) > 0:
            print df1.loc[0, "Times"]

            return df1.loc[0, "LastPrice"]
        else:
            print "Middle Rest"
            return 0

    def maxHoldTime(self, tradeTable, imex):
        sql = "select * from " + tradeTable + " limit 1820"
        df1 = pd.read_sql(sql, self.db.conn)

        tt = TimeTransfer()
        maxHoldTimes = [20, 30, 40, 50]

        for maxHoldTime in maxHoldTimes:
            df1['maxHoldTime'] = maxHoldTime
            df1['isMaxHoldTime'] = map(lambda x: 1 if x > maxHoldTime else 0, df1['HoldTime'])

            df1['LMInTimes'] = map(lambda x: tt.time_to_long(x)+maxHoldTime, df1["InTimes"])
            df1['LMLastPrice'] = map(lambda x: self.seekLastPrice("Ldata201306", x), df1['LMInTimes'])
            df1['MRevenue'] = map(lambda x, y, z: y - z if x > 0
                else z - y, df1['isLong'], df1['LMLastPrice'], df1['InLastPrice'])

            df1['MRevenue'] = map(lambda x, y, z: z if x > maxHoldTime else y, df1['HoldTime'], df1['Revenue'], df1['MRevenue'])


            print df1

            cols = list(df1)
            new_cols = []
            for col in cols:
                new_cols.append(col)
                if col == "Revenue":
                    new_cols.append("MRevenue")
                elif col == "OutLastPrice":
                    new_cols.append("LMLastPrice")
            df1 = df1[new_cols]

            imex.save_df_mysql(df1, "stats20171204_maxHoldTimeNo", False)

    #根据条件进行sql查询,可以按照table或者元素分类的返回存储了df的字典
    def basic_stats(self, db, table_list, title_list, groupByContents, contents, conditions, return_type, new_index):
        sql0 = "select "
        sql_gb = ""
        sql_cd = ""
        if len(groupByContents)>0:
            sql0 += "distinct "
            sql_gb = " group by "
            for gbc in groupByContents:
                sql0 += gbc + ", "
                sql_gb += gbc + ", "
        if len(contents)>0:
            for c in contents:
                sql0 += c + ", "
        sql0 = sql0[:-2]
        if len(conditions) > 0:
            sql_cd = " where "
            for condition in conditions:
                sql_cd += condition + " and "
            sql_cd = sql_cd[:-5]
        i = 0
        df_rs = {}
        for table, title in zip(table_list, title_list):
            sql = sql0 + " from " + table + sql_cd + sql_gb[:-2]
            print sql
            df_temp = pd.read_sql(sql, db.conn)
            if len(new_index) > 0:
                df_temp = df_temp.set_index(new_index)

            if return_type == "ByTable":
                df_rs.setdefault(table, df_temp)
            elif return_type == "ByContent":
                for content in contents:
                    if i == 0:
                        df_rs.setdefault(content, df_temp[content])
                    else:
                        df_rs[content] = pd.concat([df_rs[content], df_temp[content]], axis=1, join="outer")
                    df_rs[content].rename(columns={content: title, 0: title_list[0]}, inplace=True)

            i += 1
        return df_rs

    #对参数向量进行归一化处理
    def normalization_paras(self, allParas_means_stds, paras):
        for key in allParas_means_stds:
            mean = allParas_means_stds[key][0]
            std = allParas_means_stds[key][1]
            paras[key] = (paras[key] - mean)/std

        return paras

    #判断现在测试的参数是否收敛
    def is_stable_paras(self):
        pass

    #找到最适合的几组参数,进行下一次迭代
    def pick_good_paras(self, trade_tableName, date, minDayTradeNum):
        revenue_tableName = "revenue" + trade_tableName[10:]
        print revenue_tableName
        #self.save_revenue_mysql(imex_remote, trade_tableName, 100000, revenue_tableName, "", False, [])

        condition = ["ComputeLantency", "IntervalNum", "MuUpper", "MuLower", "lnLastPriceThreshold",
                     "A"]
        df = self.stats_revenue(revenue_tableName, condition, 'Revenue')
        '''
        path = "/home/emily/桌面/stockResult/stats" + str(date) + "/"
        if not os.path.exists:
            os.makedirs(path)
        imex.save_df_csv(df, path, "stats_" + revenue_tableName + ".csv")
        '''
        print "************"

        df_good_candicate = df[df['avgDayTradeNum'] >= minDayTradeNum]
        df_good = df_good_candicate.sort_values(by='total_revenue', ascending=False)
        print "df_good: -------------------------"
        print df_good[:5]




if __name__ == '__main__':
    S = StatsRevenue()
    db = DB('localhost', 'stockresult', 'root', '0910@mysql')
    db_remote = DB("10.141.221.124", "stockresult", "root", "cslab123")
    imex = ImExport(db)
    imex_remote = ImExport(db_remote)
    MLtags = ['pro_model', 'acc_model', 'eff_model']
    filenames = ["statsRevenue.csv", "statsHoldTime.csv"]
    titles = ["Revenue", "Time"]
    #S.bestMiddleTimeRevenue("stats20171127_varyA", imex, "/home/emily/桌面/stockResult/stats20171129/normal/",
     #                   filenames, plt, "line", titles)
    #S.stats_maxWin("revenue20171201_maxHoldTime_20", "stats20171201_maxHoldTime_20")
    #S.save_revenue_mysql(imex_remote, "tradeinfos20180124_c_i", 100000, "revenue20180124_c_i", "", False, MLtags)

    #S.maxHoldTime("stats20171120_varyA", imex)
    #print S.seekLastPrice("Ldata201306", "1370577600")
    '''
    maxHoldTimes = [20,30,40,50]
    for maxHoldTime in maxHoldTimes:
        sql = "select * from " + "stats20171204_maxHoldTimeNo_" + str(maxHoldTime)
        imex.mysqlToCSV(sql, 100000, "/home/emily/桌面/stockResult/stats20171204/",
                        "stats20171204_maxHoldTimeNo_" + str(maxHoldTime) + ".csv")
    '''

    '''
    df_rs = S.basic_stats(["`revenue20171228_ML0.2_1.0_notune`", "`revenue20171228_ML0.4_1.8_notune`",
                  "`revenue20171228_ML0.6_2.0_notune`", "`revenue20171228_ML0.8_2.4_notune`"],
                  ["0.2_1.0", "0.4_1.8", "0.6_2.0", "0.8_2.4"], ["mid(InTimes,1,8)"],
                  ["std(RealProfitF)", "avg(RealProfitF)"],
                  [], "ByContent", 'mid(InTimes,1,8)')

    for key in df_rs:
        print key
        print df_rs[key]
        print "**************"

    '''

    Reveunes = ['Revenue']
    for Reveune in Reveunes:
        print Reveune

        condition = [ "ComputeLantency", "IntervalNum","MuUpper", "MuLower", "lnLastPriceThreshold", "A"]
        tables = ["revenue20180124_c_i"]
        path = "/home/emily/桌面/stockResult/stats20180130/"
        if not os.path.exists:
            os.makedirs(path)

        for table in tables:
            df = S.stats_revenue(table, condition, Reveune)
            S.pick_good_paras("tradeinfos20180124_c_i","", 15)
            IM = ImExport(S.db)

            IM.save_df_csv(df, path, "stats_" + table + ".csv")




