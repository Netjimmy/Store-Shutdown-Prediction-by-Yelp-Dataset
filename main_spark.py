import utils
import json
import datetime
import numpy
from textblob import TextBlob

from feature import *
from build_dicts import meta
# import model
# import config
# import CrossValidator

from pyspark import SparkConf, SparkContext, SQLContext
# from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.evaluation import BinaryClassificationMetrics, MulticlassMetrics


from pyspark.mllib.classification import LogisticRegressionWithSGD, SVMWithSGD
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql.functions import col

from pyspark.ml.linalg import Vectors as MLVectors
from pyspark.ml.linalg import VectorUDT
from pyspark.mllib.linalg import Vectors as MLLibVectors
from pyspark.mllib.regression import LabeledPoint


def main(sc):

    train_id = utils.load("data_id/train.p")
    test_id = utils.load("data_id/test.p")

    meta(train_id)

    train_id = [[idx] for idx in train_id]
    test_id = [[idx] for idx in test_id]

    sqlContext = SQLContext(sc)
    train_f = sqlContext.createDataFrame(train_id, ['biz_id'])
    test_f = sqlContext.createDataFrame(test_id, ['biz_id'])

    # Register user defined functions
    # city = udf(lambda b_id: get_city(b_id), StringType())
    state = udf(lambda b_id: MLVectors.dense(get_state(b_id)), VectorUDT())
    stars = udf(lambda b_id: get_stars(b_id), FloatType())
    popularity = udf(lambda b_id: get_popularity(b_id), IntegerType())
    name_size = udf(lambda b_id: get_name_size(b_id), IntegerType())
    name_polar = udf(lambda b_id: get_name_polar(b_id), FloatType())
    pos_neg_score = udf(lambda b_id: MLVectors.dense(get_PosNeg_score(b_id)), VectorUDT())
    # clarity = udf(lambda b_id: get_clarity(b_id), ArrayType(FloatType()))
    elite_cnt = udf(lambda b_id: get_elite_cnt(b_id), IntegerType())
    label = udf(lambda b_id: get_y(b_id), IntegerType())

    # Generate feature columns
    # data_f = data_f.withColumn("city", city(data_f['biz_id']))
    train_f = train_f.withColumn("state", state(train_f['biz_id']))
    train_f = train_f.withColumn("stars", stars(train_f['biz_id']))
    train_f = train_f.withColumn("popularity", popularity(train_f['biz_id']))
    train_f = train_f.withColumn("name_size", name_size(train_f['biz_id']))
    train_f = train_f.withColumn("name_polar", name_polar(train_f['biz_id']))
    train_f = train_f.withColumn("pos_neg_score", pos_neg_score(train_f['biz_id']))
    # data_f = data_f.withColumn("clarity", clarity(data_f['biz_id']))
    train_f = train_f.withColumn("elite_cnt", elite_cnt(train_f['biz_id']))
    train_f = train_f.withColumn("y", label(train_f['biz_id']))
    train_f.show(5)

    # Generate feature columns
    test_f = test_f.withColumn("state", state(test_f['biz_id']))
    test_f = test_f.withColumn("stars", stars(test_f['biz_id']))
    test_f = test_f.withColumn("popularity", popularity(test_f['biz_id']))
    test_f = test_f.withColumn("name_size", name_size(test_f['biz_id']))
    test_f = test_f.withColumn("name_polar", name_polar(test_f['biz_id']))
    test_f = test_f.withColumn("pos_neg_score", pos_neg_score(test_f['biz_id']))
    test_f = test_f.withColumn("elite_cnt", elite_cnt(test_f['biz_id']))
    test_f = test_f.withColumn("y", label(test_f['biz_id']))
    test_f.show(5)

    # One-hot encoding
    # encoder = OneHotEncoder(inputCol="state", outputCol="stateVec")
    # train_f = encoder.transform(train_f)
    train_f.show(5)
    # test_f = encoder.transform(test_f)
    test_f.show(5)


    # Assemble columns to features
    assembler = VectorAssembler(
    inputCols=["state","stars","popularity","name_size","name_polar","pos_neg_score","elite_cnt"],
    outputCol="features")

    train_f = assembler.transform(train_f)
    train_f.show(5)
    test_f = assembler.transform(test_f)
    test_f.show(5)

    train_f = train_f.filter(train_f.y != -1)
    test_f = test_f.filter(test_f.y != -1)


    train_d = (train_f.select(col("y"), col("features")) \
                .rdd \
                .map(lambda row: LabeledPoint(float(row.y), MLLibVectors.fromML(row.features))))
    m = SVMWithSGD.train(train_d)
    predictionAndLabels = test_f.rdd.map(lambda row: (float(m.predict(MLLibVectors.fromML(row.features))), float(row.y)))
    # Grid search for best params and model
    # scores = {}
    # max_score = 0
    # for m in model_list:
    #     print ('run', m)
    #     evaluator = BinaryClassificationEvaluator()
    #     cv = CrossValidator(estimator=model_list[m],
    #                 estimatorParamMaps=params_list[m],
    #                 evaluator=evaluator,
    #                 numFolds=3)
    #     cv.fit(train)
    #     scores[m] = cv.get_best_score()
    #     if scores[m] > max_score:
    #         op_params = params_list[m][cv.get_best_index()]
    #         op_model = cv.get_best_model()
    #         op_m_name = m

    # predictionAndLabels = test.map(lambda lp: (float(op_model.predict(lp.features)), lp.y))

    # Instantiate metrics object
    bi_metrics = BinaryClassificationMetrics(predictionAndLabels)
    mul_metrics = MulticlassMetrics(predictionAndLabels)

    # Area under precision-recall curve
    print("Area under PR = %s" % bi_metrics.areaUnderPR)
    # Area under ROC curve
    print("Area under ROC = %s" % bi_metrics.areaUnderROC)
    # Confusion Matrix
    print ("Confusion Matrix")
    print (mul_metrics.confusionMatrix().toArray())

    # Overall statistics
    precision = mul_metrics.precision()
    recall = mul_metrics.recall()
    f1Score = mul_metrics.fMeasure()
    accuracy = mul_metrics.accuracy
    print("Summary Stats")
    print("Precision = %s" % precision)
    print("Recall = %s" % recall)
    print("F1 Score = %s" % f1Score)
    print ("Accuracy = %s" % accuracy)

    # Individual label stats
    labels = [0,1]
    for label in labels:
        print("Class %s precision = %s" % (label, mul_metrics.precision(label)))
        print("Class %s recall = %s" % (label, mul_metrics.recall(label)))
        # print("Class %s F1 Measure = %s" % (label, mul_metrics.fMeasure(label)))

if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName("Yelp")
    conf = conf.setMaster("local[4]")
    sc = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
