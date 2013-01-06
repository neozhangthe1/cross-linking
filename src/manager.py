'''
Created on Dec 20, 2012

@author: Yutao
'''
import os
import subprocess
import shutil

if __name__ == "__main__":
    ITERATION = 20
    err_cnt = 0
    for itr in range(ITERATION):
        print "%sth iterating......" % itr

        os.chdir(str(itr))
        if not os.path.exists("simrank.py"):
            shutil.copyfile("../simrank.py", "simrank.py")

        ferr = open("simrank_error.log", 'wb')
        fout = open("simrank_out.log", "wb")
        status = subprocess.call(["python","simrank.py"], stdout=fout, stderr=ferr)
        if status != 0:
            err_cnt += 1
            print "Error, please check error.log for more information."
        ferr.close()
        fout.close()
        os.chdir("..")
    print "Done. %s errors. " % err_cnt

    