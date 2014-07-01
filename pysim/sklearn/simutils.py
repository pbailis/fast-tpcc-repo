from numpy.linalg import norm
from numpy import average, var
from scipy.sparse import *
from pylab import *
import urllib
from zipfile import ZipFile
import gzip
import pandas as pd
import os

data_dir = "./data/"
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
    
# Download the URL into data_dir + result_dir
def get_dataset(url, result_dir):
    fname = url.split("/")[-1]
    if not os.path.isfile(data_dir + fname):
        print "Downloading " + fname 
        data = urllib.urlretrieve(url, data_dir + fname)
    if not os.path.exists(data_dir + result_dir):    
        if fname[-3:] == "zip":
            ZipFile(data_dir + fname).extractall(data_dir + result_dir)
        if fname[-3:] == ".gz":
            fin = gzip.open(data_dir + fname)
            fout = open(data_dir + result_dir, 'wb')
            fout.writelines(fin)
            fin.close()
            fout.close()

def make_binary_df(data):
    data2 = pd.DataFrame()
    for col_name in data:
        if data[col_name].dtype is dtype('O'):
            # Remove whitespace in column
            old_col = data[col_name].map(str.strip)
            # determine the unique values
            values = old_col.unique() # pd.unique(old_col.values)
            if size(values) == 2:
                # Ensure a canoncial ordering
                values = sort(values)
                # special case where we only need one column
                new_col = 2.0 * (old_col == values[1]) - 1.0
                data2[col_name] = new_col
            else:
                # create a new binary column for each value
                for v in values:
                    new_col = 2.0 * (old_col == v) - 1.0
                    data2[col_name + "_is_" + v] = new_col
        else:
            data2[col_name] = data[col_name]
    return data2
