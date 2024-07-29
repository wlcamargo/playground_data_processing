from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from time import sleep
import time
from pyfiglet import Figlet


# Configuração do Spark
conf = SparkConf()
conf.setAppName("Hello Sparkanos")

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

f = Figlet(font='standard')
print(f.renderText('Hello Sparkanos!'))

time.sleep(10)

f = Figlet(font='standard')
print(f.renderText('RAULLL!'))
time.sleep(3)

print(f.renderText('RAULLL!'))
time.sleep(3)