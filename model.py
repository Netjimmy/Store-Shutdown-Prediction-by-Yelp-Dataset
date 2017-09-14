from pyspark.mllib.classification import LogisticRegressionWithSGD, SVMWithSGD
from pyspark.ml.tuning import ParamGridBuilder

model_list = {
    "Linear-SVM": SVMWithSGD(),
    "LogisticRegression": LogisticRegressionWithSGD()
}

params_list = {
    "LogisticRegression" : ParamGridBuilder()\
    .addGrid(LogisticRegressionWithSGD.regParam, [0.1, 0.01]) \
    .addGrid(LogisticRegressionWithSGD.step, [0.1, 0.01])\
    .addGrid(LogisticRegressionWithSGD.miniBatchFraction, [0.1, 0.5, 1.0])\
    .addGrid(LogisticRegressionWithSGD.regType, ['l1', 'l2', None])\
    .addGrid(LogisticRegressionWithSGD.convergenceTol, [0.001, 0.0001])
    .build(),
    "Linear-SVM": ParamGridBuilder()\
    .addGrid(SVMWithSGD.regParam, [0.1, 0.01]) \
    .addGrid(SVMWithSGD.step, [0.1, 0.01])\
    .addGrid(SVMWithSGD.miniBatchFraction, [0.1, 0.5, 1.0])\
    .addGrid(SVMWithSGD.regType, ['l1', 'l2', None])\
    .addGrid(SVMWithSGD.convergenceTol, [0.001, 0.0001])
    .build()
}

if __name__ == "__main__":
    # SVMWithSGD.train(training_data, estimatorParamMaps=grid)
    # SVMWithSGD.predict(test_data)

    # LogisticRegressionWithSGD.train(data, estimatorParamMaps=grid)
    # LogisticRegressionWithSGD.predict(test_data)
