# Apache [Spark](https://spark.apache.org/) (and [Hadoop](http://hadoop.apache.org/)) using [python3](https://www.python.org/), [PyCharm](https://www.jetbrains.com/pycharm/) and [Jupiter](http://jupyter.org/) notebook

## Setup

#### Install python3 and java8

1: Check installed version 

`$ java -version`

2: Optionally - if java 1.7 or java 1.9, install java 8
 
``` 
$ brew update
$ brew tap caskroom/versions
$ brew cask install java8
```

3: Optionally - uninstall java 1.9

`$ brew cask uninstall java`

4: Check python version

`$ python --version`

5: Optionally - if version is *not* 3, update/install

`$ brew install python`

#### Install Spark
Source: http://jmedium.com/pyspark-in-mac/

1: [Download](http://spark.apache.org/downloads.html) Apache Spark, select a pre-built for Hadoop package. Unzip the tar: 

`@ tar xvf spark-2.3.0-bin-hadoop2.7.tar`
 

2: Then move the installation

`@  sudo mv spark-2.3.0-bin-hadoop2.7 /usr/local`

3: Update .bash_profile

```
export SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.7"
export PATH=$SPARK_HOME/bin:$PATH
```

4: Check installation

```
$ pyspark
Python 3.6.4 (default, Mar  1 2018, 18:36:50) 
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.3.0
      /_/

Using Python version 3.6.4 (default, Mar  1 2018 18:36:50)
SparkSession available as 'spark'.
>>> 
```

5: Install juniper notebook

`@ pip install jupyter`

6: Find pyspark to make it importable ([findspark doc on github](https://github.com/minrk/findspark))

`@ pip install findspark`

7: Start Jupyter notebook in web-browser.

`@ jupyter notebook`

 Test installation by open simple_example.ipynb
 
#### Install and setup PyCharm
[Download] (https://www.jetbrains.com/pycharm/download/#section=mac) and install PyCharm
Open this project :-)

Use python3 - change settings
  
```
File -> Default Settings -> Project Interpreter
For project: PyCharm -> Preferences -> Project: <name> -> Project Interpreter
```
