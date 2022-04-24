from statistics import mode
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
import csv

ASSIGN_VIEW_CONST = 3
ASSIGN_BUY_CONST = 100
SPLIT_COEF = [0.8, 0.2]

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

conf = SparkConf().setAll([('spark.executor.memory', '250g'),
                            ('spark.executor.cores', '7'),
                            ('spark.cores.max', '8'),
                            ('spark.driver.memory', '100g'),
                            ("spark.app.name", "bloomreach_challenge")])
spark = SparkSession.builder.appName('bloomreach_challenge').getOrCreate()
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("rating", FloatType(), True)])
spark.sparkContext.setCheckpointDir("checkpoint/")
ratingsDF = spark.read.csv("R.csv", header=False, sep=",", schema=schema, nanValue='')
ratingsDF.printSchema()

(training, test) = ratingsDF.randomSplit(SPLIT_COEF)

# By default ALS assumes explicit feedback. One can change that by setting implicitPrefs=True.
als = ALS(rank=10, maxIter=10, regParam=1.0,
          userCol="user_id", itemCol="item_id", ratingCol="rating", coldStartStrategy="drop", nonnegative=True, checkpointInterval=2)

# Tune model
param_grid = ParamGridBuilder()\
                .addGrid(als.rank, [10, 50, 75, 100])\
                .addGrid(als.maxIter, [5, 30, 75, 100])\
                .addGrid(als.regParam, [.01, .05, .1, .15])\
                .build()

evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")


# Build cross validation
tvs = TrainValidationSplit(estimator=als, estimatorParamMaps=param_grid, evaluator=evaluator)

# Fit ALS to training data and extract best
model = tvs.fit(training)
best_model = model.bestModel

# Generate predictions for testing part of data
predictions = best_model.transform(test)
rmse = evaluator.evaluate(predictions)

print("RMSE => " + str(rmse))
print("-------------")
print("Rank: " + str(best_model.rank))
#print("MaxIter: " + best_model._java_obj.parent().getIterMax())
#print("RegParam: " + best_model._java_obj.parent().getRegParam())


# After best model acquired generate final csv for all
top_5_recommend = best_model.recommendForAllUsers(5)
output = csv.writer(open('final.csv', "w"))
output.writerow(['customer_id','product_id'])
for row in top_5_recommend.collect():
    for data in row.recommendations:
        # print(users_reverse[row.user_id])
        # print(items_reversed[data[0]])
        output.writerow([users_reverse[row.user_id],  items_reversed[data[0]]])
