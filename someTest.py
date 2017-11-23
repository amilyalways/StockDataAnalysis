from datetime import datetime
from dateutil.parser import parse
import pandas as pd
from Utility.DB import DB
from matplotlib import pyplot as plt

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
sql = "select InA FROM revenue20171120_varyA"
df = pd.read_sql(sql, db.conn)
print df.describe()

InAs = ["0.0017153", "0.00171845", "0.00171852", "0.00171855", "0.00171958"]
'''InAs = ["0.0017", "0.001725", "0.0018"]'''

sql = "select Revenue FROM revenue20171120_varyA"
df = pd.read_sql(sql, db.conn)
df1 = df.describe()
df.plot()
plt.show()

for InA in InAs:
    sql = "select Revenue FROM revenue20171121_fixedA where InA=" + InA
    df2 = pd.read_sql(sql, db.conn).describe()
    df2.rename(columns={'Revenue': InA}, inplace=True)

    df1 = pd.concat([df1, df2], axis=1)
print df1



