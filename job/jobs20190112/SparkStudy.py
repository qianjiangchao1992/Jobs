from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName("MySpark").setMaster("local")
    sc= SparkContext(conf=conf)
    words=sc.textFile("D:\\linux\\download\\README.txt")
    print(words.collect())

