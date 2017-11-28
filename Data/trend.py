import pandas as pd
from Utility.DB import DB
import numpy as np
from Utility.TimeTransfer import TimeTransfer
from Data.ImExport import ImExport

db = DB('localhost', 'stockresult', 'root', '0910@mysql')
tt = TimeTransfer()
imex = ImExport(db)
for j in range(0, 3):
    table = "data20131" + str(j)
    print "------------" + table + "--------------------"

    sql = "select * from " + table
    df = pd.read_sql(sql, db.conn)
    '''
    record_num = db.select(table, "count(*)")[0]['count(*)']
    print record_num
    intervalNum = 6
    for i in range(1):

        rows = [x for x in range(i, record_num, intervalNum)]
        df1 = df.ix[rows]

        df1['diff'] = df1['LastPrice'].diff(-1)

        df1['abs_diff'] = np.abs(df1['diff'])
        print df1['abs_diff'].describe()
        print "85%: ", df1['abs_diff'].quantile(0.85)
        print "90%: ", df1['abs_diff'].quantile(0.9)
        print "95%: ", df1['abs_diff'].quantile(0.95)
        print "98%: ", df1['abs_diff'].quantile(0.98)
        print "99%: ", df1['abs_diff'].quantile(0.99)
    '''

    df['L_time'] = map(lambda x: tt.time_to_long(x), df["Times"])
    imex.save_df_mysql(df, "L"+table, False)





