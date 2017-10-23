from datetime import datetime
from dateutil.parser import parse
import pandas as pd

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
