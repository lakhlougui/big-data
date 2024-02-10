import findspark
findspark.init()

from pyspark import sparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import kafkaUtils
import json 
import time

if __name__=="__main__":
    spark=SparkSession.builder.master("local").appName("kafka spark demo").getOrCreate()

    sc=spark.sparkContext

    ssc=StreamingContext(sc,20)

    message=kafkaUtils.createDirectStream(ssc,topics=['testtopic'],kafkaParams={'metadata.broker.list':'localhost:9092'})

    data = message.map(lambda x: x[1])  

    def functordd(rdd):
        try:
            rdd1=rdd.map(lambda x:json.loads(x))
            df=spark.read.json(rdd1)
            df.show()
            df.createOrReplaceTempView("test")
            df1=spark.sql("select iss_position.latitude,iss_position.longitude,message,timestamp from test")
            df1.write.format('csv').mode('append').save("testing")
        except:
            pass
            
    
    data.foreachRDD(functordd)

    ssc.start()
    ssc.awaitTermination()

