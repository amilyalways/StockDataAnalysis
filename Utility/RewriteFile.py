
def rewriteFile(path, filename, new_rows):

    newfilename = "new" + filename
    with open(path+filename, "r+") as f_old:
        with open(path+newfilename, "w") as f_new:
            id = 0
            for line in f_old.readlines():
                if id in new_rows:
                    f_new.writelines(new_rows[id] + "\n")
                else:
                    f_new.writelines(line)

                id += 1


if __name__ == '__main__':
    path = "/Users/songxue/Desktop/"
    arr_filename = ["3-2-(8-12).sql"]
    new_rows = {
        24: "",
        25: "CREATE TABLE IF NOT EXISTS `171016allpara` ("
    }

    for filename in arr_filename:
        rewriteFile(path, filename, new_rows)
