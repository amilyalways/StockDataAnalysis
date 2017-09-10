import time
import math


class TimeTransfer:

    format = '%Y%m%d-%H:%M:%S'

    def __init__(self):
        pass

    def set_format(self, format):
        self.format = format

    def time_to_long_no(self, time1):
        return time.mktime(time.strptime(time1[:17], self.format))

    def time_to_long(self, time1):
        l = self.time_to_long_no(time1)
        l = l + int(time1[18:]) * 0.001
        return l

    def time_to_ms_long_no(self, time1):
        return 1000*self.time_to_long_no(time1)

    def time_to_ms_long(self, time1):
        return 1000 * self.time_to_long(time1)
    
    def long_to_time_no(self, l):
        time1 = time.localtime(l)
        return time.strftime(self.format, time1)

    def long_to_time(self, l):
        if math.modf(l)[0] == 0.0:
            tail = " 0"
        else:
            tail = " 500"
        time1 = time.localtime(l)
        return time.strftime(self.format, time1) + tail

    def ms_long_to_time_no(self, l):
        return self.long_to_time_no(l/1000)

    def ms_long_to_time(self, l):
        return self.long_to_time(l/1000)

if __name__ == '__main__':
    print "Test TimeTransfer"

    tt = TimeTransfer()
    time1 = "20130603-09:20:10 0"
    l1 = tt.time_to_long(time1)
    print l1
    print tt.long_to_time(l1)
    l2 = tt.time_to_ms_long(time1)
    print l2
    print tt.ms_long_to_time(l2)
    time2 = "20130603-09:10:10"
    l3 = tt.time_to_long_no(time2)
    print l3
    print tt.long_to_time_no(l3)
    l4 = tt.time_to_ms_long_no(time2)
    print l4
    print tt.ms_long_to_time_no(l4)









