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
    gps_schema = StructType([
        StructField('id', StringType(), True),
        StructField('device_id', StringType(), True),
        StructField('timestamp', TimeStampType(), True),
        StructField('speed', DoubleType(), True),
        StructField('vehicle_type', StringType(), True)  
    ])

   # Traffic schema
    traffic_schema = StructType([
        StructField('id', StringType(), True),
        StructField('device_id', StringType(), True),
        StructField('camera_id', StringType(), True),
        StructField('location', StringType(), True),
        StructField('timestamp', TimeStampType(), True),
        StructField('snapshot', StringType(), True)
    ])
   
   # Weather schema
    weather_schema = StructType([
        StructField('id', StringType(), True),
        StructField('device_id', StringType(), True),
        StructField('timestamp', TimeStampType(), True),
        StructField('temperature', DoubleType(), True),
        StructField('weather_condition', StringType(), True),
        StructField('precipitation', DoubleType(), True),
        StructField('wind_speed', DoubleType(), True),
        StructField('humidity', IntegerType(), True),
        StructField('air_quality_index', DoubleType(), True),
    ])
    
   # Emergency schema
    emergency_schema = StructType([
        StructField('id', StringType(), True),
        StructField('device_id', StringType(), True),
        StructField('incident_id', TimeStampType(), True),
        StructField('type', StringType(), True),
        StructField('timestamp', TimeStampType(), True),
        StructField('location', StringType(), True),
        StructField('status', StringType(), True),
        StructField('description', StringType(), True)
    ])
    
    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootsrap.servers', 'broker:29092')
                .option('subcribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(values as STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes'))
    
    vehicle_df = read_kafka_topic('vehicle_data', vehicle_schema).alias('vehicle')
    vehicle_df = read_kafka_topic('vehicle_data', vehicle_schema).alias('vehicle')
    vehicle_df = read_kafka_topic('vehicle_data', vehicle_schema).alias('vehicle')