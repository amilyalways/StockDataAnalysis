import csv
import matplotlib.pyplot as plt
'''
urls = [1,2,3]
titles = ['a','b','c']
data = []
for url, title in zip(urls, titles):
    row = {
        'url': url,
        'title': title

    }
    data.append(row)
with open('C:\\Users\\songxue\\Desktop\\stats_info\\test.csv','w',newline='') as csvfile:
    fieldnames = ['url', 'title']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data)
'''
'''
def plot_sub_figure(x,y,title,xlabel,ylabel, auto,linewidth, xfontsize, labelsize,z,zlabel):
    if auto:
        plt.plot(y)
        plt.xticks(range(len(x)), x, rotation=45)
        plt.grid()
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)

    else:

        fig, ax1 = plt.subplots(figsize=(60, 35))

        plt.plot(y,'r')
        ax1.tick_params('y', colors='r')
        #ax1.set_yticks(fontsize=xfontsize)
        plt.xticks(range(len(x)), x)
        plt.title(title, fontsize=45)
        ax1.grid()
        ax1.set_xlabel(xlabel, fontsize=labelsize)
        ax1.set_ylabel(ylabel, fontsize=labelsize)
        #ax1.set_axis(fontsize=30)

        ax2 = ax1.twinx()
        ax2.plot(z,'b')
        ax2.set_ylabel(zlabel)
        ax2.tick_params('y',colors='b')

x = [1,2,3,4,5,10]
y = [1,1,2,5,3,7]
z = [3,3,3,5,6,0]

plot_sub_figure(x,y,"tete","x","y",False,10,40,45,z,"z")
plt.show()
'''

import pymysql
# connect to db
def connect_db(host, db, username, password):
    config = {
        'host': host,
        'port': 3306,
        'user': username,
        'password': password,
        'db': db,
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor,
    }
    conn = pymysql.connect(**config)
    return conn

conn = connect_db('localhost','stockresult','root','0910mysql@')
cur = conn.cursor()

sql = "select Times from innervalue2day where Times like '%0 0'"
cur.execute(sql)
result = cur.fetchall()
new_times = {}
for re in result:
    new_times.setdefault(re['Times'], re['Times'][:19])
    #print(new_times)

for times in new_times:
    sql1 = "update innervalue2day set Times='" + new_times[times] + "' where Times='" + times + "'"
    cur.execute(sql1)
    conn.commit()
