# -*- coding: utf-8 -*-
def rewriteFile(path, filename, new_rows):

    newfilename = "new" + filename
    with open(path+filename, "r+") as f_old:
        with open(path+newfilename, "w") as f_new:
            id = 0
            for line in f_old.readlines():
                if id in new_rows:
                    print id
                    print line
                    print new_rows[id]
                    f_new.writelines(new_rows[id] + "\n")
                else:
                    f_new.writelines(line)

                id += 1

def FiletoMysql(path, arr_filename):
    for filename in arr_filename:
        print "source " + path + "new" + filename


if __name__ == '__main__':
    path = "/home/emily/下载/"
    '''
    arr_filename = ["1-1-(3-7).sql", "1-1-(8-12).sql", "1-2-(3-7).sql",
                    "1-2-(8-12).sql", "2-1-(3-7)-missing.sql", "2-1-(8-12).sql",
                    "2-2-(3-7).sql", "2-2-(8-12).sql", "3-1-(3-7)-missing.sql",
                    "3-1-(8-12).sql", "3-2-(3-7).sql", "3-2-(8-12).sql"]
    '''
    arr_filename = ["20171123expectAandAA-18zu-(8-12).sql"]

    new_rows = {
        21: "",
        24: "CREATE TABLE IF NOT EXISTS `20171123expectaandaa`"
    }

    for filename in arr_filename:
        print filename
        rewriteFile(path, filename, new_rows)
        print "********************************"


    FiletoMysql(path, arr_filename)