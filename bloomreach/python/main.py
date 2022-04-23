from curses.textpad import rectangle
from itertools import count
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.ml.recommendation import ALS
import csv

ASSIGN_VIEW_CONST = 1
ASSIGN_BUY_CONST = 1

# Prepare ratings


userreader = csv.reader(open('../data/target_group.csv'), delimiter=',')
users = dict()
users_reverse = dict()
user = 1
for row in userreader:
    if (row != "customer_id"):
        users["".join(row)] = user
        users_reverse[user] = "".join(row)
        user+=1

itemreader = csv.reader(open('../data/catalog_items.csv'), delimiter=',')
items = dict()
items_reversed = dict()
item = 1
for row in itemreader:
    if (row[0] != "product_id"):
        items["".join(row[0])] = item
        items_reversed[item] = "".join(row[0])
        item+=1

weights = dict(dict())
relationreader = csv.reader(open('../data/visited_products.csv'), delimiter=',')
for row in relationreader:
    if (row[0] != "customer_id"):
        if row[0] in users:
            if row[0] in weights:
                if row[1] in weights[row[0]]:
                    weights[row[0]][row[1]] = weights[row[0]][row[1]] + ASSIGN_VIEW_CONST
                else:
                    weights[row[0]][row[1]] = ASSIGN_VIEW_CONST
            else:
                weights[row[0]] = dict()
                weights[row[0]][row[1]] = ASSIGN_VIEW_CONST

purchasedreader = csv.reader(open('../data/purchased_producs.csv'), delimiter=',')
for row in purchasedreader:
    if (row[0] != "customer_id"):
        if row[0] in users:
            if row[0] in weights:
                if row[1] in weights[row[0]]:
                    weights[row[0]][row[1]] = weights[row[0]][row[1]] + ASSIGN_BUY_CONST
                else:
                    weights[row[0]][row[1]] = ASSIGN_BUY_CONST
            else:
                weights[row[0]] = dict()
                weights[row[0]][row[1]] = ASSIGN_BUY_CONST


finalcsv = csv.writer(open('R.csv', "w"))
for produser in weights: 
    for prodprod in weights[produser]:
        if prodprod not in items: 
            items[prodprod] = item
            items_reversed[item] = prodprod
            item+=1
        finalcsv.writerow([users[produser], items[prodprod], weights[produser][prodprod]])



# ALGORITHM
# https://everdark.github.io/k9/notebooks/ml/matrix_factorization/matrix_factorization.nb.html#53_spark_mllib

# conf = SparkConf().setAll([('spark.executor.memory', '1g'),
#                            ('spark.executor.cores', '1'),
#                            ('spark.cores.max', '2'),
#                            ('spark.driver.memory', '2g'),
#                            ("spark.app.name", "bloomreach_challenge")])
spark = SparkSession.builder.appName('bloomreach_challenge').getOrCreate()
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("rating", FloatType(), True)])

ratingsDF = spark.read.csv("R.csv", header=False, sep=",", schema=schema, nanValue='')
ratingsDF.withColumn('user_id', ratingsDF['user_id'].cast("int"))
ratingsDF.withColumn('item_id', ratingsDF['item_id'].cast("int"))

ratingsDF.printSchema()

# By default ALS assumes explicit feedback. One can change that by setting implicitPrefs=True.
als = ALS(rank=3, maxIter=10, regParam=.01,
          userCol="user_id", itemCol="item_id", ratingCol="rating")
model = als.fit(ratingsDF)

top_5_recommend = model.recommendForAllUsers(5)

output = csv.writer(open('final.csv', "w"))
output.writerow(['customer_id','product_id'])
for row in top_5_recommend.collect():
    for data in row.recommendations:
        # print(users_reverse[row.user_id])
        # print(items_reversed[data[0]])
        output.writerow([users_reverse[row.user_id],  items_reversed[data[0]]])