# Enron exercise

If you don't have pyenv;

```
brew update
brew install pyenv
```

Choose your python version

```
pyenv install 3.9.0
```

After that, a virtual environment, pointing to this Python version, can be created (please note an explicit reference to the Python version):

```
python3 -m venv venv_test
```

Install dependencies and create a new venv with:
```
pip install -r requirements.txt
```

### PySpark

Download latest Spark from the Spark site https://spark.apache.org/downloads.html

Example :
```
tar -xvf Downloads/spark-2.1.0-bin-hadoop2.7.tgz`

Add to bashrc:
export SPARK_HOME = /home/hadoop/spark-2.1.0-bin-hadoop2.7
export PATH = $PATH:/home/hadoop/spark-2.1.0-bin-hadoop2.7/bin
export PYTHONPATH = $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH
export PATH = $SPARK_HOME/python:$PATH

```


## Download and Extract
Make sure the file is downloaded local to the scripts
```
python3 download_enron.py
```

Run this command

#extract from

```
python extract.py
```

a CSV containing all the email data person  will output to "./export" folder

## transform

run the following command to launch spark and run the analysis job
```
python transform.py
```
