## Abstract
We build a business failure model to predict if a business will survive another half-year. The data is from "The Yelp Dataset Challenge 2017". Collecting data from Yelp users, our model can come up with promising results (Kappa coefficient = 0.156) in business failure classification problems. In addition, PCA can further improve our performance by projecting our features into lower dimensions. More details please refer to [poster](https://github.com/Netjimmy/Store-Shutdown-Prediction-by-Yelp-Dataset/blob/master/stores_shutdown_prediction_poster.pdf) and [report](https://github.com/Netjimmy/Store-Shutdown-Prediction-by-Yelp-Dataset/blob/master/stores_shutdown_prediction_report.pdf)

## Data Underdstanding
We use data from "The Yelp Dataset Challenge 2017", which is an open data competition held by Yelp. The dataset contains 140k business and 4.1 millions reviews across 11 cities in four countries around the world. The total data size is around 4.8 GB. The original dataset has three major json files of our interest: business, reviews and users.

## Feature Selection and Generation
<img width="750" alt="screen shot 2018-03-21 at 9 57 47 pm" src="https://user-images.githubusercontent.com/15644582/37746933-f6e6c174-2d52-11e8-9a16-1c78817f3fe7.png">

#### Text-based Features
For unstructure data like reviews, we tokenized it into words and calculated the positive and negtive scores. We also select the top 100 most frequent nouns and adjective as input features. 

#### Features of Temporal Information
We ran linear regression on the time series to get _star_trend_

#### Features of Spatial Information
We count the stores in same catogory in 1 mile as competitor and generated feature _competitor_total_

## Modeling Comparision
To evaluate imbalanced data, Kappa Coefficient is introduced. The definition please see [wikipedia](https://en.wikipedia.org/wiki/Cohen%27s_kappa).
<img width="626" alt="screen shot 2018-03-21 at 10 09 25 pm" src="https://user-images.githubusercontent.com/15644582/37747235-934c1acc-2d54-11e8-865d-fbb2c2243828.png">

Random Forest gives the best result of 0.156 by Kappa coefficient and the AUC is 0.564, while Logistic Regression has the best AUC. The reason of this inconsistence is that Kappa score emphasizes agreement between prediction and the true value.
