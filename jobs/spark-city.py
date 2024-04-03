import findspark
findspark.init()

from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName('SmartCityStreaming')\
    .config('spark.jars.packages',
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1',
            'org.apache.hadoop:hadoop-aws:3.3.1',
            'com.amazonaws:aws-java-sdk:1.11.469')\
    .config('spark.hadoop.fs.s3a.impl',
            'org.apache.hadoop.fs.s3a.S3AFileSystem')\
    .config('spark.hadoop.fs.s3a.access.key', configuration.get('AWS_ACCESS_KEY'))\
    .config('spark.hadoop.fs.s3a.secret.key', configuration.get('AWS_SECRET_KEY'))\
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.impl.SimpleAWSCredentialsProvider')\
    .getOrCreate()

    # Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel('WARN')

    # Vehicle schema
    vehicle_schema = StructType([
        StructField('id', StringType(), True),
        StructField('device_id', StringType(), True),
        StructField('timestamp', TimeStampType(), True),
        StructField('location', StringType(), True),
        StructField('speed', DoubleType(), True),
        StructField('direction', StringType(), True),
        StructField('make', StringType(), True),
        StructField('model', StringType(), True),
        StructField('year', IntegerType(), True),
        StructField('fuel_type', StringType(), True)
    ])

    # Gps schema
