from StockDataAnalysis.Utility.DB import DB

db = DB('localhost', 'stockresult','root','0910mysql@')
target = [("tradeinfos_InnerValue20130625new","tradeinfos20130625new","innervalue20130625new"),
          ]
for t in target:
    sql = "create table " + t[0] + " (select " + t[1] + ".Times," + t[1] + ".LastPrice," + t[1] + ".isOpen," \
          + t[1] + ".isLong," + t[1] + ".ComputeLantency," + t[1] + ".IntervalNum," + t[1] + ".MuUpper," \
          + t[1] + ".MuLower," + t[2] + ".Mu," + t[2] + ".Sigma," + t[2] + ".LossingStopUp," + t[2] + ".LossingStopDown " \
          "from " + t[1] + " inner join " + t[2] + " on " + t[1] + ".Times=" + t[2] +".Times and " \
           + t[1] + ".ComputeLantency=" + t[2] +".ComputeLantency and " + t[1] + ".IntervalNum=" + t[2] +".IntervalNum and " \
          + t[1] + ".MuUpper=" + t[2] + ".MuUpper )"
    print(sql)
    db.cur.execute(sql)
    db.conn.commit()
