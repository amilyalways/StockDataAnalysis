# -*- coding: utf-8 -*-
from sklearn.cluster import KMeans
from sklearn.externals import joblib
from sklearn import cluster
import numpy as np
import pandas as pd
from Utility.DB import DB
from matplotlib import pyplot as plt

db = DB('localhost', 'stockresult', 'root', '0910@mysql')

content = ""
condition = ""
for i in range(1, 21):
    content += "profit" + str(i) + ", "
    condition += " and abs(profit" + str(i) + ")<2"
content = content[:-2]
print content


sql1 = "SELECT distinct " + content + " FROM revenue20171225_ML where Sign=1 and RealProfitF>0" + condition \
       +" group by " + content
df1 = pd.read_sql(sql1, db.conn)
new_cols = {}
for i in range(21):
    new_cols.setdefault("profit"+str(i), i)
df1.rename(columns=new_cols, inplace=True)
df2 = df1.T
print df2
plt.plot(df2)
plt.show()

# 聚类为4类
estimator = KMeans(n_clusters=6)
# fit_predict表示拟合+预测，也可以分开写
res = estimator.fit_predict(df1)

# 预测类别标签结果
lable_pred = estimator.labels_
# 各个类别的聚类中心值
centroids = estimator.cluster_centers_
# 聚类中心均值向量的总和
inertia = estimator.inertia_

print lable_pred
rsClass = {}
for label in lable_pred:
    if label not in rsClass:
        rsClass.setdefault(label, 1)
    else:
        rsClass[label] += 1
print rsClass
print centroids
print inertia
i = 0
for centroid in centroids:
    if rsClass[i] > 1000:
        plt.plot(centroid, label=str(i) + ": " + str(rsClass[i]))
    i += 1

plt.grid()
plt.xlim(0, 19)
plt.yticks(np.arange(-2.4, 7, 0.2))
plt.xticks(np.arange(0, 20, 1))
plt.legend(loc='upper right', prop={'size': 10})
plt.show()