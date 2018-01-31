import commands
import collections
import time
from Data.ImExport import ImExport
from Utility.DB import DB
from Data.StatsRevenue import StatsRevenue
import math
import numpy as np



class RunTrade:
    allParas = collections.OrderedDict()
    paras_names = []
    allParas_means_stds = collections.OrderedDict()
    paras_tableName = ""
    start_id = 0
    minDayTradeNum = 15
    min_dist_convergence = 0.5
    top_paras_num = 5
    neighbor_paras_num = 2




    def __init__(self, fixed_paras, thresholds, dbs,):
        self.allParas = fixed_paras['allParas']
        self.paras_names = self.allParas.keys()
        for key in self.allParas:
            means = np.mean(allParas[key])
            std = np.std(allParas[key])
            self.allParas_means_stds.setdefault(key, (means, std))
        self.paras_tableName = paras_tableName
        self.start_id = fixed_paras['start_id']

        self.minDayTradeNum = thresholds['minDayTradeNum']
        self.min_dist_convergence = thresholds['min_dist_convergence']
        self.top_paras_num = thresholds['top_paras_num']
        self.neighbor_paras_num = thresholds['neighbor_paras_num']

        self.db = dbs['db']
        self.db_remote = dbs['db_remote']

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




    def addNewParas_mysql(self, db, newParas, trade_start_day, trade_end_day, parastableName, destTableName):
        sql_num = "select count(*) from " + parastableName
        db.cur.execute(sql_num)
        new_id = db.cur.fetchall()[0]['count(*)']
        print new_id
        startId = new_id + 1
        endId = new_id + len(new_paras)
        destTableName += "_" + str(startId) + "_" + str(endId)
        for rowParas in newParas:
            new_id += 1
            values = str(new_id)
            for paras in rowParas:
                values += ", " + str(paras)
            values += ", '" + destTableName + "'" + ", " + trade_start_day + ", " + trade_end_day + ", 0"
            db.insert(parastableName, values)


        return startId, endId, destTableName

    def isFinishTrade(self, db, startId, endId, parastableName):
        sql = "select min(isFinished) from " + parastableName + \
              " where paras_id  between " + str(startId) + " and " + str(endId)
        db.cur.execute(sql)
        rs = db.cur.fetchall()[0]['min(isFinished)']
        print sql
        print rs
        if rs == 1:
            return True
        else:
            return False

    def iterationTrade_FindGoodParas(self, new_paras, trade_start_day, trade_end_day, paras_tableName):
        is_stable = False
        date = time.strftime('%Y%m%d', time.localtime(time.time()))

        while not is_stable:
            for key in self.paras:
                trade_tableName = "tradeinfos" + s + "_" + key

                startId, endId, trade_tableName = self.addNewParas_mysql(
                    db_remote, new_paras, trade_start_day, trade_end_day, paras_tableName, trade_tableName)
                print "startId: " + str(startId)
                print "endId: " + str(endId)

                while True:
                    isFinishTrade = self.isFinishTrade(db_remote, startId, endId, paras_tableName)

                    if isFinishTrade:
                        is_stable, new_paras = stats.pick_good_paras(trade_tableName, date)
                        break
                    else:
                        print "Trade is not finished... Please wait..."
                        time.sleep(5)
                        sql = "update " + paras_tableName + \
                              " set isFinished=1"
                        db_remote.cur.execute(sql)
                        db_remote.conn.commit()

        return new_paras


if __name__ == '__main__':

    allParas = collections.OrderedDict()
    allParas['ComputeLantency'] = [6, 8]
    allParas['IntervalNum'] = [10, 200]
    allParas['MuUpper'] = [1, 2, 3]
    allParas['MuLower'] = [0.5, 0.6]
    allParas['lnLastPriceThreshold'] = [0.003, 0.002]
    allParas['A'] = [0.001, 0.002]

    paras_tableName = "paras_need_run"
    fixed_paras = {
        'allParas': allParas,
        'paras_tableName': paras_tableName,
        'start_id': 5
    }

    thresholds = {
        'minDayTradeNum': 15,
        'min_dist_convergence': 0.5,
        'top_paras_num': 5,
        'neighbor_paras_num': 2
    }

    dbs = {
        'db': DB('localhost', 'stockresult', 'root', '0910@mysql'),
        'db_remote': DB("10.141.221.124", "stockresult", "root", "cslab123")
    }

    run_trade = RunTrade(fixed_paras, thresholds, dbs)

    print run_trade.allParas
    print run_trade.paras_names
    print run_trade.allParas_means_stds
    print run_trade.paras_tableName
    print run_trade.start_id

'''

    initialParas = {
        'ComputeLantency': [6,8],
        'IntervalNum': [10,200],
        'MuUpper': [],
        'MuLower': [],
        'lnLastPriceThreshold': [],
        'A': []
    }
    paras = initialParas



    stats = StatsRevenue()



    new_paras = [[6, 840, 0.01, 0.02, 0.003, 0.1], [20, 240, 0.01, 0.02, 0.003, 0.1]]
    trade_start_day = "201303"
    trade_end_day = "201307"
    stable_paras = run_trade.iterationTrade_FindGoodParas(new_paras, trade_start_day, trade_end_day, paras_tableName)

    date = time.strftime('%Y%m%d', time.localtime(time.time()))
    stable_trade_tables = "tradeinfos" + date + "_stable"
    trade_start_day = "201308"
    trade_end_day = "201312"
    allParas = {
        'ComputeLantency': [6, 8],
        'IntervalNum': [10, 200],
        'MuUpper': [1, 2, 3],
        'MuLower': [0.5, 0.6],
        'lnLastPriceThreshold': [0.003, 0.002],
        'A': [0.001, 0.002]
    }
    run_trade.addNewParas_mysql(db_remote, stable_paras, trade_start_day, trade_end_day, paras_tableName, stable_paras)
'''


   



















