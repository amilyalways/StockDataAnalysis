# -*- coding: utf-8 -*-

from datetime import datetime
from dateutil.parser import parse
import pandas as pd
from Utility.DB import DB
from matplotlib import pyplot as plt
from Utility.TimeTransfer import TimeTransfer
from Data.ImExport import ImExport
import numpy as np
import os
'''
stamp = datetime(2017,9,20,9,21,10, 5)
print stamp
print str(stamp)
print stamp.day, stamp.year, stamp.month
print stamp.time()

t = "2017-09-10"
print parse(t)

m = "2017-11-10 12:10:09"
pm = parse(m)
print pm.year, pm.month, pm.day, pm.time()
w = pm - stamp
print w.days, w.seconds, w.microseconds

x = pd.date_range( end="2018-01-29", periods=10, freq="BM")
print x
'''


db = DB('localhost', 'stockresult', 'root', '0910@mysql')
imex= ImExport(db)
'''
sql = "select InA FROM revenue20171127_varyA"
df = pd.read_sql(sql, db.conn)
print df.describe()

#InAs = ["0.0017153", "0.00171845", "0.00171852", "0.00171855", "0.00171958"]
#InAs = ["0.0017", "0.001725", "0.0018"]
InAs = ["0.0017", "0.0017153", "0.00171845", "0.00171852", "0.00171855",
        "0.00171958", "0.001725", "0.0018", "0.002", "0.003"]

sql = "select Revenue FROM revenue20171127_varyA"
df = pd.read_sql(sql, db.conn)
df1 = df.describe()


for InA in InAs:
    sql = "select Revenue FROM revenue20171127_fixedA_allParas where InA=" + InA
    df2 = pd.read_sql(sql, db.conn).describe()
    df2.rename(columns={'Revenue': InA}, inplace=True)

    df1 = pd.concat([df1, df2], axis=1)
print df1
'''

'''
sql3 = "select InTimes, HoldTime from stats20171120_varyA"
df3 = pd.read_sql(sql3, db.conn)

tt = TimeTransfer()
df3['LInTime'] = map(lambda x: tt.time_to_long(x), df3["InTimes"])

df3['diff'] = pd.rolling_apply(df3['LInTime'], 2, lambda x: x[1]-x[0])


print df3
#imex.save_df_csv(df3, "/home/emily/桌面/", "diffTime.csv")
df4 = pd.read_csv("/home/emily/桌面/diffTime.csv")
df5 = df4[df4.IntervalTime < 5000]['IntervalTime']
print df5.describe(percentiles=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.98])
'''

'''
sql = "SELECT Revenue, MRevenue, MiddleRevenue " \
      "FROM stats20171204_maxHoldTimeNo_50 where isMaxHoldTime=1 and MRevenue<Revenue and MRevenue>-2000"
df = pd.read_sql(sql, db.conn)
print df.describe(percentiles=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.98])
sql1 = "SELECT Revenue, MRevenue, MiddleRevenue " \
      "FROM stats20171204_maxHoldTimeNo_50 where isMaxHoldTime=1 and MRevenue>Revenue and MRevenue>-2000"
df1 = pd.read_sql(sql1, db.conn)
print df1.describe(percentiles=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.98])
'''

'''
sql = "SELECT distinct ComputeLantency,IntervalNum,MuUpper,InA, count(*), avg(Revenue) " \
      "FROM revenue20171205_fixedA where InTimes like '%-11:%' and OutTimes like '%-13:%' " \
      "group by ComputeLantency,IntervalNum,MuUpper,InA"
df1 = pd.read_sql(sql, db.conn)
print df1
imex.save_df_csv(df1, "/home/emily/桌面/stockresult/", "SpecialTrade.csv")
'''

'''
sql = "select distinct ComputeLantency,IntervalNum,lnLastPriceThreshold, count(*),avg(Mu) " \
      "from 20171213fixedaamuinlastprice group by ComputeLantency,IntervalNum,lnLastPriceThreshold"
df = pd.read_sql(sql, db.conn)
print df

sql1 = "ALTER TABLE `stockresult`.`paras20171219_fixedA_1` ADD INDEX `search_index` " \
       "(`ComputeLantency` ASC, `IntervalNum` ASC, `lnLastPriceThreshold` ASC)"
db.cur.execute(sql1)
db.conn.commit()
'''

'''
tablenames = ["revenue20171227_ML0.2_1.0", "revenue20171227_ML0.4_1.8", "revenue20171227_ML0.6_2.0",
              "revenue20171227_ML0.8_2.4"]

content = ""
for i in range(1, 21):
    content += "profit" + str(i) + ", "
content = content[:-2]
print content
#content = "RealProfitF"
content = "distinct RealProfitF, avg(HoldTime)*3"
#content = "count(*)"
i = 0
for tablename in tablenames:
    sql1 = "SELECT " + content + " FROM `" + tablename + "` where Sign=1 group by RealProfitF"
    print sql1
    df1 = pd.read_sql(sql1, db.conn)
    #df2 = df1.describe(percentiles=[0.02, 0.05, 0.1, 0.2, 0.3, 0.4, 0.42, 0.45, 0.48, 0.5, 0.52, 0.55, 0.58, 0.6, 0.7, 0.8, 0.9, 0.95, 0.98])
    df2 = df1
    if i > 0:
        df3 = pd.concat([df3, df2], axis=1, join='inner')
    else:
        df3 = df2
    i += 1
print df3
imex.save_df_csv(df3, "/home/emily/桌面/stockResult/stats20171227/", "stats20171227_h.csv")
'''

'''
filenames = ["rst-full-year-lr-up-0.0004.csv", "rst-full-year-lr-down-0.0004.csv", "rst-full-year-up-0.0004.csv",
             "rst-full-year-down-0.0004.csv", "rst-full-year-lr-raw-up-0.0004.csv", "rst-full-year-lr-raw-down-0.0004.csv"]
path = "/home/emily/下载/"

i = 0
for filename in filenames:
    df1 = pd.read_csv(path+filename)
    #df1.rename(columns={'0': filename[14:-11]}, inplace=True)


    if i > 0:
        df3 = pd.concat([df3, df1], axis=1, join='inner')
    else:
        df3 = df1
    i += 1


print df3
print "*******"

sql1 = "select * from `revenue20171227_ML0.6_2.0`"
df4 = pd.read_sql(sql1, db.conn)
#df4.rename(columns={'HoldTime*3':'HoldTime'}, inplace=True)
print df4
print "************************"

df3 = pd.concat([df4, df3], axis=1, join='inner')
print df3
'''
#imex.save_df_mysql(df3, "revenue20171228ML0.6_2.0category0.0004", True)

'''
sql = "SELECT count(*), avg(a.RealProfitF), avg(b.RealProfitF), avg(a.HoldTime)*3 " \
      "FROM stockresult.`revenue20171229_MLdown0.2_1.0_notune` as a, `revenue20171228_ML0.8_2.4_notune` as b " \
      "where a.Sign=1 and b.Sign=1  and a.InTimes=b.InTimes and a.OutTimes=b.OutTimes"
df1 = pd.read_sql(sql, db.conn)
print df1

#(mean-avg(mean))/std(mean), (LastPrice-avg(LastPrice))/std(LastPrice)
#(mean-3.511745)/5.218067, (LastPrice-2390.856635)/123.657932
sql = "SELECT mu,LastPrice  FROM 20171213fixedaamuinlastpriceformean" \
      " where ComputeLantency=20 and IntervalNum =6 limit 5000"
df = pd.read_sql(sql, db.conn)
print df
plt.plot(df['mu'])
ax2 = plt.twinx()
ax2.plot(df['LastPrice'], 'red')
plt.grid()
plt.xticks(np.arange(0, 5000, 200))

plt.show()
#imex.mysqlToCSV(sql, 100000, "/home/emily/桌面/", "revenue20171230.csv")

sql1 = "select vector, vectorforzero from 20180104fixedaamuinlastpriceformean where length(vectorforzero)>40"
df1 = pd.read_sql(sql1, db.conn)
df2 = map()
'''
'''
db_remote = DB("10.141.221.124", "stockresult", "root", "cslab123")

sql2 = "SELECT Times, mu, isOpen, inmu FROM 20180119fixedaaandz where Times like '201303%' and MuUpper=0.0004 and lnLastPriceThreshold=0.0002"
df_trade = pd.read_sql(sql2, db_remote.conn)

sql3 = "select Times, mu from 20180121fixedaamuinlastprice where Times like '201303%' and ComputeLantency=35 and lnLastPriceThreshold=0.0002"
df_mu = pd.read_sql(sql3, db_remote.conn)

df_trade.rename(columns={'mu':'t_mu'}, inplace=True)
print df_trade
print "******************************"
print df_mu
print df_trade.describe()
print "******************************"
df_rs = pd.merge(df_mu, df_trade, how='outer', on='Times')
print df_rs

path = "/home/emily/桌面/stockResult/stats20180122/"
if not os.path.exists(path):
    os.makedirs(path)
imex.save_df_csv(df_rs, path, "mu_compare35_201303.csv")
'''
path = "/home/emily/桌面/stockResult/stats20180125/"
sql_revenue = "select * from revenue20180124_c_i where ComputeLantency=6 and IntervalNum=840 " \
              "and MuUpper=0.0008 and MuLower=-0.0004 and lnLastPriceThreshold=0.003 and A=0.006"
sql_revenue = "select * from revenue20180124_c_i where ComputeLantency=30 and IntervalNum=240 " \
              "and MuUpper=0.0002 and MuLower=-0.0001 and lnLastPriceThreshold=0.003 and A=0.015"
#sql_revenue = "select * from revenue20180124"
df_revenue = pd.read_sql(sql_revenue, db.conn)
imex.save_df_csv(df_revenue, path, "revenue20180124_C_I_30_240.csv")
'''
days = ['02','05', '06', '07','08','20','21','27','28','30']
path = "/home/emily/桌面/stockResult/stats20180125/"
if not os.path.exists(path):
    os.makedirs(path)
for day in days:
    sql_trend = "select * from data201308 where Times like '201308" + day + "%'"
    df_trend = pd.read_sql(sql_trend, db.conn)
    imex.save_df_csv(df_trend, path, "data201308"+day+".csv")
'''

'''
sql4 = "select * from data201306 where Times like '20130603%'"
df_tren = pd.read_sql(sql4, db.conn)
imex.save_df_csv(df_tren, path, "trend20130603.csv")
'''
'''
path = "/home/emily/下载/data20180110/"
filename = "price_with_trade-up--0.8-2.4.csv"
df = pd.read_csv(path+filename)
df['new_Times'] = map(lambda x: x[:-2], df['Times'])
print df.head(10)
print pd.to_datetime('20130318 09:15:21')
print pd.to_datetime('20130318-09:15:21')
df['new_Times'] = pd.to_datetime(df['new_Times'], format='%Y%m%d-%H:%M:%S')
df = df.set_index('new_Times', inplace=True)


print df.head(10)

df2 = df.resample('3T')
print df2.head(20)
'''
