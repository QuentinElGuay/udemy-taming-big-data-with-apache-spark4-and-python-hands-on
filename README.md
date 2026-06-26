# udemy-taming-big-data-with-apache-spark4-and-python-hands-on
Repository for the exercises of the Udemy Course "Taming Big Data with Apache Spark 4 and Python - Hands On!"


### Configure your environment
Acording to [the Spark documentation](https://spark.apache.org/docs/latest/) at the time I write this document:
> Spark runs on Java 17/21, Scala 2.13, Python 3.10+, and R 3.5+ (Deprecated). When using the Scala API, it is necessary for applications to use the same version of Scala that Spark was compiled for. Since Spark 4.0.0, it’s Scala 2.13.

To run Spark 4 on Codespace like I do, I recommend using the already installed version of Java 21:
```bash
$ sdk default java 21.0.10-ms
setting java 21.0.10-ms as the default version for all shells.
```

Download Spark 4.1.2:
```bash
wget https://downloads.apache.org/spark/spark-4.1.2/spark-4.1.2-bin-hadoop3.tgz
wget https://downloads.apache.org/spark/spark-4.1.2/spark-4.1.2-bin-hadoop3.tgz.sha512
sha512sum -c spark-4.1.2-bin-hadoop3.tgz.sha512
tar xvf spark-*.tgz
rm spark-*.tgz
```

Move the content of the unpacked directory `spark-4.1.2-bin-hadoop3/` to the `/opt/spark/` directory:
```bash
sudo mv spark-4.1.2-bin-hadoop3 /opt/spark
```

[Optional] Rename the `/opt/spark/conf/log4j2.properties.template` file and change the `rootLogger.level` property to `error` using the nano text editor (press Ctrl+X, then Y, then Enter to save and exit):
```bash
mv /opt/spark/conf/log4j2.properties.template /opt/spark/conf/log4j2.properties
nano /opt/spark/conf/log4j2.properties
```

Add variables at the end of your `~/.bashrc` file:
```bash
cat << 'EOF' >> ~/.bashrc
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
export PYSPARK_PYTHON=python3
EOF
```

Apply those new environment variables:
```bash
source ~/.bashrc
```

Install `uv` for environment management, init the project then install some required libraries:
```
pip install uv
uv init
uv add py4j pandas pyarrow
```
