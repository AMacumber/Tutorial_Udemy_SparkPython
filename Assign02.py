import findspark
findspark.init('c:/users/andy/spark')

from pyspark import SparkConf, SparkContext

data_folder = "C:/Users/Andy/Dropbox/FactoryFloor/Repositories/Tutorial_Udemy_SparkPython/Course_Resources/"

# configure your Spark context; master node is local machine
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")

# create a spark context object
sc = SparkContext(conf = conf)

# path to file of interest
file_to_open = data_folder + "customer-orders.csv"

# load the file; textFile breaks up a data file so that each row represents a single value in an RDD
input = sc.textFile(file_to_open)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    totalamount = float(fields[2])
    return (customerID, totalamount)

rdd = input.map(parseLine)
totalsByID = rdd.reduceByKey(lambda x, y: (x + y))
totalsByIDSorted = totalsByID.map(lambda x: (x[1], x[0])).sortByKey()


results = totalsByIDSorted.collect()
for result in results:
    total = str(result[0])
    customerID = result[1]
    if customerID:
        print(customerID, total)
