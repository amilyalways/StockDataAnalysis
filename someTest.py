# -*- coding: utf-8 -*-

from datetime import datetime
from dateutil.parser import parse
import pandas as pd
from Utility.DB import DB
from matplotlib import pyplot as plt
from Utility.TimeTransfer import TimeTransfer
from Data.ImExport import ImExport
import numpy as np
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
db1 = DB("10.141.221.124", "stockresult", "root", "cslab123")
sql2 = "show tables"
db1.cur.execute(sql2)
rs = db1.cur.fetchall()
print rs

url = "10.141.221.124/公共资料库/数据挖掘/20180108/交易结果/price_with_trade-down-0.2--1.0.csv"
df = pd.read_csv(url)
print df