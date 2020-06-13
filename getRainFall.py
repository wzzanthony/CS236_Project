from pyspark import SparkContext 
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
import csv
import time
import sys
import os 

sc = SparkContext()
sqlContext = SQLContext(sc)

#get Files under the given path
def getFiles(path):
    files= os.listdir(path)
    files_list = []
    for file in files:
        if not os.path.isdir(file):
            files_list.append(os.path.join(path,file))
    return files_list


#get the three columns(USAF, CTRY and STATE)
def getLocations(file_name):
    df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(file_name)
    df = df.select('USAF', 'CTRY', 'STATE')
    df = df.filter("CTRY = 'US' and STATE is not null")
    df = df.select('USAF', 'STATE').dropDuplicates(['USAF'])
    content = [['USAF', 'STATE']]
    for row in df.rdd.collect():
        ret = []
        ret.append("'" + row['USAF'])
        ret.append(row['STATE'])
        content.append(ret)
    csv_file_name = "locations.csv"
    csv_file = open(csv_file_name, "w", newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerows(content)
    csv_file.close()
    
"""
def generateDict():
    dic = {}
    df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('locations.csv')
    print(df.dtypes)
    print(len(df.rdd.collect()))
    for row in df.rdd.collect():
        dic["'" + row['USAF']] = row['STATE']
    print(len(dic))
"""

def preDeal(file = "test.txt", mode = 0):
    dic = {'A': 4, 'B':2, 'C':4/3, 'D':1, 'E':1, 'F':1, 'G':1, 'H':0, 'I':0}
    columns = ["USAF", "YEARMODA", "PRCP"]
    content = []
    if mode == 0:
        write = "w"
        content.append(columns)
    else:
        write = "a"
    with open(file, "r") as f:
        for line in f:
            if "STN---" not in line:
                ret = []
                value = line.split()
                stn = "'" + value[0]
                ret.append(stn)
                time =value[2][4:6]
                ret.append(time)
                prcp = value[-3]
                if prcp[-1] == '9':
                    continue
                prcp_sign = dic[prcp[-1]]
                prcp_value = float(prcp[:-1]) * prcp_sign
                ret.append(prcp_value)
                content.append(ret)
    
    csv_file_name = "middle_result.csv"
    csv_file = open(csv_file_name, write, newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerows(content)
    csv_file.close()

def batchDeal(file_list):
    mode = [0,1,1,1]
    for i in range(len(mode)):
        preDeal(file_list[i], mode[i])

#This process will generate recordings.csv file which contains three columns
#STATE: The Name of State
#YEARMODA: the month
#PRCP: average rainfall
def getRainFallData(file = "middle_result.csv"):
    df1 = sqlContext.read.format('com.databricks.spark.csv').options(header="true", inferschema='true').load(file)
    #df = df.toDF("111", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22")
    df1 = df1.groupby("USAF", "YEARMODA").agg({"PRCP":"avg"})
    df2 = sqlContext.read.format('com.databricks.spark.csv').options(header="true", inferschema='true').load("locations.csv")
    df  = df2.join(df1, on = ['USAF'], how = "inner")
    df = df.select("STATE", "YEARMODA","avg(PRCP)")
    df = df.groupby("STATE", "YEARMODA").agg({"avg(PRCP)":"sum"}).sort("STATE", "YEARMODA", ascending = True)
    df = df.withColumnRenamed("sum(avg(PRCP))", "PRCP")
    df.toPandas().to_csv("recordings.csv", index = False, header=True)

def SortedByDifference(file):
    df =  sqlContext.read.format('com.databricks.spark.csv').options(header="true", inferschema='true').load( "recordings.csv")
    df1 = df2 = df
    df1 = df1.groupby("STATE").agg({"PRCP":"max"})
    df1 = df1.withColumnRenamed("max(PRCP)", "PRCP")
    df1 = df1.join(df, on = ["STATE","PRCP"], how = "inner")
    df1 = df1.withColumnRenamed("PRCP", "max_PRCP").withColumnRenamed("YEARMODA", "max_month")
    df2 = df2.groupby("STATE").agg({"PRCP":"min"})
    df2 = df2.withColumnRenamed("min(PRCP)", "PRCP")
    df2 = df2.join(df, on = ["STATE","PRCP"], how = "inner")
    df2 = df2.withColumnRenamed("PRCP", "min_PRCP").withColumnRenamed("YEARMODA", "min_month")
    df = df1.join(df2, on = ['STATE'], how = "inner").dropDuplicates(['STATE'])
    df = df.withColumn("Diff", df.max_PRCP - df.min_PRCP)
    df = df.sort("Diff", ascending = True)
    df.toPandas().to_csv(file, index = False, header=True)

if __name__ == "__main__":
    start = time.time()
    location_folder = sys.argv[1]
    recording_folder = sys.argv[2]
    output_folder = sys.argv[3]
    location_file = getFiles(location_folder)[0]
    recording_files = getFiles(recording_folder)
    output_file_path = os.path.join(output_folder, "sorted_file.csv")

    getLocations(location_file)
    batchDeal(recording_files)
    getRainFallData()
    SortedByDifference(output_file_path)

    end = time.time()
    print("it takes {}s in total".format(end - start))


