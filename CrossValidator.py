class CrossValidator():
    def __init__(self, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3):
        self.estimator = estimator
        self.estimatorParamMaps = estimatorParamMaps
        self.evaluator = evaluator
        self.numFolds = numFolds
        self.bestIndex = None
        self.bestScore = None
        self.score = {}

    def fit(self, dataset):
        numModels = len(self.estimatorParamMaps)
        metrics = [0.0] * numModels
        n = dataset.shape[0]
        for i in range(self.nFolds):
            low = int((1.0*i/self.nFolds)*n)
            high = int((1.0*(i+1)/self.nFolds)*n)
            train = dataset[0:low].append(dataset[high:])
            validatation = dataset[low:high]
            for j in range(numModels):
                model = self.estimator.fit(train, self.estimatorParamMaps[j])
                # model = self.estimator.train(train, estimatorParamMaps=self.estimatorParamMaps[j])
                metric = self.evaluator.evaluate(model.transform(validation, self.estimatorParamMaps[j]))
                metrics[j] += metric
        for i in range(numModels):
            self.score[self.estimatorParamMaps[i]] = metrics[i]/self.nFolds
        if self.evaluator.isLargerBetter():
            bestIndex = np.argmax(metrics)
        else:
            bestIndex = np.argmin(metrics)
        self.bestIndex = bestIndex
        self.bestScore = self.score[self.estimatorParamMaps[bestIndex]]

    def get_scores(self):
        return self.score

    def get_best_index(self):
        return self.bestIndex

    def get_best_score(self):
        return self.bestScore

    def get_best_model(self, dataset):
        return self.estimator.fit(dataset, self.estimatorParamMaps[self.bestIndex])
