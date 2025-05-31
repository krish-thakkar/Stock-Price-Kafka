import os
import sys
from pathlib import Path
import findspark
findspark.init()
from pyspark.sql.functions import *
from pyspark.sql.types import *
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql import DataFrame
from cassandra.cluster import Cluster
import urllib.request
import json
from textblob import TextBlob

def setup_hadoop_binaries():
    """Download and setup Hadoop binaries for Windows"""
    hadoop_dir = Path("C:/hadoop")
    bin_dir = hadoop_dir / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)

    # Download winutils.exe if not present
    winutils_path = bin_dir / "winutils.exe"
    if not winutils_path.exists():
        print("Downloading winutils.exe...")
        winutils_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/winutils.exe"
        urllib.request.urlretrieve(winutils_url, str(winutils_path))

    # Download hadoop.dll if not present
    hadoop_dll_path = bin_dir / "hadoop.dll"
    if not hadoop_dll_path.exists():
        print("Downloading hadoop.dll...")
        hadoop_dll_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/hadoop.dll"
        urllib.request.urlretrieve(hadoop_dll_url, str(hadoop_dll_path))

    # Set environment variables
    os.environ['HADOOP_HOME'] = str(hadoop_dir)
    os.environ['PATH'] = f"{str(bin_dir)};{os.environ['PATH']}"
    
    print("Hadoop binaries setup completed")

def create_spark_session():
    """Create Spark session with necessary configurations"""
    from pyspark.sql import SparkSession
    
    return SparkSession.builder \
        .appName("NewsStreamingSentiment") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .master("local[*]") \
        .getOrCreate()



def analyze_sentiment(text):
    """Analyze sentiment of text using TextBlob with enhanced processing"""
    if not text:
        return 0.0
    
    # Clean text - remove extra whitespace, convert to lowercase
    text = ' '.join(text.split()).lower()
    
    blob = TextBlob(text)
    # Get both polarity and subjectivity
    polarity = blob.sentiment.polarity
    subjectivity = blob.sentiment.subjectivity
    
    # Weight the sentiment by subjectivity
    # This helps distinguish between factual and opinion-based content
    weighted_sentiment = polarity * (0.5 + 0.5 * subjectivity)
    
    return weighted_sentiment

def create_sentiment_udf():
    """Create UDF for sentiment analysis with TextBlob"""
    def get_sentiment(text):
        if text is None:
            return None
        return analyze_sentiment(text)
    
    return udf(get_sentiment, DoubleType())


def process_batch(df, epoch_id):
    """Process each batch of news data with sentiment analysis"""
    try:
        if df.rdd.isEmpty():
            print(f"Batch {epoch_id}: No data received")
            return
        
        print(f"\nProcessing batch {epoch_id}")
        
        # Register sentiment UDF
        sentiment_udf = create_sentiment_udf()
        
        # Calculate sentiment scores
        sentiment_df = df \
            .withColumn("title_sentiment", sentiment_udf("title")) \
            .withColumn("description_sentiment", sentiment_udf("description")) \
            .withColumn("overall_sentiment", 
                       (col("title_sentiment") + col("description_sentiment")) / 2) \
            .withColumn("sentiment_label", 
                       when(col("overall_sentiment") >= 0.1, "positive")
                       .when(col("overall_sentiment") <= -0.1, "negative")
                       .otherwise("neutral"))
        
        # Calculate sentiment statistics per company
        company_sentiment = sentiment_df \
            .groupBy("company") \
            .agg(
                avg("overall_sentiment").alias("avg_sentiment"),
                count("*").alias("article_count"),
                avg(when(col("sentiment_label") == "positive", 1).otherwise(0))
                    .alias("positive_ratio"),
                avg(when(col("sentiment_label") == "negative", 1).otherwise(0))
                    .alias("negative_ratio")
            )
        
        print("\nCompany Sentiment Analysis:")
        company_sentiment.show()
        
        # Show most positive and negative articles
        print("\nMost Positive Articles:")
        sentiment_df \
            .orderBy(desc("overall_sentiment")) \
            .select("company", "title", "overall_sentiment") \
            .limit(3) \
            .show(truncate=False)
            
        print("\nMost Negative Articles:")
        sentiment_df \
            .orderBy("overall_sentiment") \
            .select("company", "title", "overall_sentiment") \
            .limit(3) \
            .show(truncate=False)
        
        # Save results to Cassandra
        save_to_cassandra(sentiment_df, 'stock_analysis', 'news_sentiment')
        
    except Exception as e:
        print(f"Error processing batch {epoch_id}: {str(e)}")

def save_to_cassandra(df: DataFrame, keyspace: str, table: str):
    """Save sentiment analysis results to Cassandra"""
    # Connect to the Cassandra cluster
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect(keyspace)

    # Create the table if it doesn't exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        company text,
        timestamp timestamp,
        description text,
        description_sentiment float,
        overall_sentiment float,
        sentiment_label text,
        title text,
        title_sentiment float,
        PRIMARY KEY (company, timestamp)
    )"""
    session.execute(create_table_query)

    # Insert data
    insert_query = session.prepare(
        f"""INSERT INTO {table} 
            (company,timestamp, description, description_sentiment, overall_sentiment, sentiment_label, title, title_sentiment)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
    )

    # Insert each row
    for row in df.collect():
        session.execute(insert_query, (
            row.company,
            row.fetch_timestamp,
            row.description,
            row.description_sentiment,
            row.overall_sentiment,
            row.sentiment_label,
            row.title,
            row.title_sentiment
        ))

    print(f"Sentiment data saved to table {table} under keyspace {keyspace}.")


def setup_environment():
    """Setup required environment and dependencies"""
    import subprocess
    
    # Install required packages
    subprocess.check_call(['pip', 'install', 'textblob'])
    


def start_streaming():
    """Start the streaming process"""
    try:
        setup_environment()
        # Setup environment
        setup_hadoop_binaries()
        
        # Create Spark session
        print("Creating Spark session...")
        spark = create_spark_session()
        print("Spark session created successfully")
        
        # Define schema for incoming news data
        schema = StructType([
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("source", StringType(), True),
            StructField("url", StringType(), True),
            StructField("published_at", TimestampType(), True),
            StructField("fetch_timestamp", TimestampType(), True),
            StructField("company", StringType(), True)
        ])
        
        # Read from Kafka
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "stock_news") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Process the stream
        query = parsed_df \
            .writeStream \
            .foreachBatch(process_batch) \
            .outputMode("update") \
            .trigger(processingTime='10 seconds') \
            .start()
        
        print("Started streaming. Waiting for news data...")
        query.awaitTermination()
        
    except Exception as e:
        print(f"Error in streaming: {str(e)}")
        raise

if __name__ == "__main__":
    start_streaming()