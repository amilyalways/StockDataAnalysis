import commands
import collections
import time
from Data.ImExport import ImExport
from Utility.DB import DB
from Data.StatsRevenue import StatsRevenue
import math
import numpy as np


class RunTrade:
    paras = collections.OrderedDict()


    def __init__(self, names, allParas):
        for key in names:
            self.paras[key] = allParas[key]

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
    allParas = {
        'ComputeLantency': [6,8],
        'IntervalNum': [10,200],
        'MuUpper': [1,2,3],
        'MuLower': [0.5,0.6],
        'lnLastPriceThreshold': [0.003,0.002],
        'A': [0.001,0.002]
    }
    names = ['ComputeLantency', 'IntervalNum', 'MuUpper', 'MuLower',
             'lnLastPriceThreshold', 'A']
    allParas_means_stds = {}


    for key in allParas:
        means = np.mean(allParas[key])
        std = np.std(allParas[key])
        allParas_means_stds.setdefault(key, (means, std))
    print allParas_means_stds
    print allParas_means_stds['A'][0]
    print allParas_means_stds['A'][1]

'''
    run_trade = RunTrade(names, allParas)

    initialParas = {
        'ComputeLantency': [6,8],
        'IntervalNum': [10,200],
        'MuUpper': [],
        'MuLower': [],
        'lnLastPriceThreshold': [],
        'A': []
    }
    paras = initialParas

    db_remote = DB("10.141.221.124", "stockresult", "root", "cslab123")

    stats = StatsRevenue()

    paras_tableName = "paras_need_run"

    new_paras = [[6, 840, 0.01, 0.02, 0.003, 0.1], [20, 240, 0.01, 0.02, 0.003, 0.1]]
    trade_start_day = "201303"
    trade_end_day = "201307"
    stable_paras = run_trade.iterationTrade_FindGoodParas(new_paras, trade_start_day, trade_end_day, paras_tableName)

    date = time.strftime('%Y%m%d', time.localtime(time.time()))
    stable_trade_tables = "tradeinfos" + date + "_stable"
    trade_start_day = "201308"
    trade_end_day = "201312"
    run_trade.addNewParas_mysql(db_remote, stable_paras, trade_start_day, trade_end_day, paras_tableName, stable_paras)
'''


   



















