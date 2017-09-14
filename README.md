Follow the steps below to install requirements.

# 0. virtual environment

> virtualenv -p /usr/bin/python3.4 ML \
> source ML/bin/activate

@@@ Optional
> pip3 install jupyter \
> ipython3 kernelspec install-self \
> jupyter notebook

# 1. pyspark

Download spark-2.1.0-bin-hadoop2.7.tgz at http://spark.apache.org/downloads.html

> tar -xzf spark-2.1.0-bin-hadoop2.7.tgz \ 
> mkdir /srv
> mv spark-2.1.0-bin-hadoop2.7 /srv/spark-2.1.0 \
> ln -s /srv/spark-2.1.0 /srv/spark \
> vim ~/.bashrc
@ Copy and Paste these two lines
export SPARK_HOME=/srv/spark \\
export PATH=$SPARK_HOME/bin:$PATH
> source ~/.bashrc


