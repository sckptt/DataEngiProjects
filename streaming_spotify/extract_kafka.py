import findspark
import json
import os
import psycopg2
import redis
import spotipy
import time
from colorama import init
from termcolor import colored
from dotenv import load_dotenv
from kafka import KafkaProducer
from psycopg2 import OperationalError
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, year, month, dayofmonth, dayofweek, hour, minute
from pyspark.sql.types import StructType, StringType, ArrayType, LongType, StructField
from spotipy.oauth2 import SpotifyOAuth

findspark.init()
load_dotenv()
init()

def create_spotify_client():
	try:
		my_id = os.getenv("SPOTIPY_CLIENT_ID")
		my_secret= os.getenv("SPOTIPY_CLIENT_SECRET")
		my_redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI")

		if not my_id or not my_secret or not my_redirect_uri:
			raise ValueError("Spotify API credentials are missing! Please check your environmental variables!")
		
		my_scopes = "user-read-playback-state"
		my_sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
			client_id=my_id,
			client_secret=my_secret,
			redirect_uri=my_redirect_uri,
			scope=my_scopes))
		print(colored("Spotify client is created", "green"))
		return my_sp
	except Exception as e:
		print(colored(f"Couldn't create the spotify client due to exception {e}", "red"))
		return None

def parse_playback(playback : dict) -> dict:
	try:
		if playback["currently_playing_type"] == "episode":
			return None
		
		elif playback["currently_playing_type"] == "track":
			device_name = playback["device"]["name"]
			device_type = playback["device"]["type"]
			url = playback["context"]["external_urls"]["spotify"]
			artists_name = []
			for i in range(len(playback["item"]["artists"])):
				name = playback["item"]["artists"][i]["name"]
				artists_name.append(name)
			artists_id = []
			for i in range(len(playback["item"]["artists"])):
				artist_id = playback["item"]["artists"][i]["id"]
				artists_id.append(artist_id)
			album_name = playback["item"]["album"]["name"]
			album_picture = playback["item"]["album"]["images"][0]["url"]
			song_name = playback["item"]["name"]
			song_id = playback["item"]["id"]
			timestamp = playback["timestamp"]
			playing_type = playback["currently_playing_type"]
			print(colored("Playback is parsed successfully", "green"))
			return {
				"device_name" : device_name,
				"device_type" : device_type,
				"url" : url,
				"artists_name" : artists_name,
				"artists_id" : artists_id,
				"album_name" : album_name,
				"album_picture" : album_picture,
				"song_name" : song_name,
				"song_id" : song_id,
				"timestamp" : timestamp,
				"playing_type" : playing_type
			}
	except KeyError as e:
		print(colored(f"Key error occured: {e}", "red"))
		return None
	except Exception as e:
		print(colored(f"An error occured while parsing playback: {e}", "red"))
		return None

def check_duplicates(pp : dict, redis_client: redis.Redis) -> bool:
	song_id = pp["song_id"]
	timestamp = pp["timestamp"] / 1000
	unique_key = f"{song_id}:{timestamp}"

	if redis_client.exists(unique_key):
		return False
	else:
		redis_client.set(unique_key, "loaded", ex=600)
		return True

def create_spark_connection():
	try:
		spark_connection = SparkSession.builder \
			.appName("SpotifyTracks") \
			.master("local[*]") \
			.config("spark.driver.extraJavaOptions", "-Duser.library.path=$JAVA_HOME/lib") \
			.config("spark.executor.extraJavaOptions", "-Duser.library.path=$JAVA_HOME/lib") \
			.config("spark.java.options", "-Dfile.encoding=UTF-8") \
			.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.3.8") \
			.config("spark.ui.enabled", "false") \
			.getOrCreate()

		spark_connection.sparkContext.setLogLevel("ERROR")
		print(colored("Spark connection is created", "green"))
		return spark_connection
	except Exception as e:
		print(colored(f"Couldn't create the spark session due to exception {e}", "red"))
		return None

def create_kafka_connection(spark_connection):
	try:
		data_frame = spark_connection \
			.readStream \
			.format("kafka") \
			.option("kafka.bootstrap.servers", "localhost:9092") \
			.option("subscribe", "spotify_tracks") \
			.option("startingOffsets", "earliest") \
			.option("enable.auto.commit", "true") \
    		.option("group.id", "spotify_consumer_group") \
			.load()
		print(colored("Spark connected with Kafka successfully", "green"))
		return data_frame
	except Exception as e:
		print(colored(f"Couldn't create a dataframe due to exception {e}", "red"))
		return None

def parse_streaming_schema(data_frame):
	schema = StructType([
		StructField("device_name", StringType(), True),
		StructField("device_type", StringType(), True),
		StructField("url", StringType(), True),
		StructField("artists_name", ArrayType(StringType()), True),
		StructField("artists_id", ArrayType(StringType()), True),
		StructField("album_name", StringType(), True),
		StructField("album_picture", StringType(), True),
		StructField("song_name", StringType(), True),
		StructField("song_id", StringType(), True),
		StructField("timestamp", LongType(), True),
		StructField("playing_type", StringType(), True)])

	try:
		parsed_df = data_frame \
			.selectExpr("CAST(value AS STRING) as json_string") \
			.select(from_json(col("json_string"), schema).alias("data")) \
			.select("data.*")
		
		print(colored("Data frame is parsed successfully", "green"))
		return parsed_df
	except Exception as e:
		print(colored(f"Couldn't parse a data frame due to exception {e}", "red"))
		return None

def add_datetime_columns(data_frame):
	try:
		enriched_df = data_frame.withColumn("timestamp", from_unixtime(col("timestamp") / 1000)) \
			.withColumn("year", year(col("timestamp"))) \
			.withColumn("month", month(col("timestamp"))) \
			.withColumn("day", dayofmonth(col("timestamp"))) \
			.withColumn("weekday", dayofweek(col("timestamp"))) \
			.withColumn("hour", hour(col("timestamp"))) \
			.withColumn("minute", minute(col("timestamp")))
		# times_played = enriched_df.groupBy("song_id", "day", "month", "year").agg(count("song_id").alias("times_played"))
		# deduplicated_df = enriched_df.dropDuplicates(["song_id", "day", "month", "year"])
		# final_df = deduplicated_df.join(times_played, on=["song_id", "day", "month", "year"])
		print(colored("Data frame is enriched successfully", "green"))
		return enriched_df
	except Exception as e:
		print(colored(f"Couldn't add datetime columns due to exception {e}", "red"))
		return None

def create_postgre_connection():
	try:
		postgre_connection = psycopg2.connect(
			dbname = os.getenv("DB_NAME"),
			user = os.getenv("DB_USER"),
			password = os.getenv("DB_PASSWORD"),
			host = os.getenv("DB_HOST"),
			port = os.getenv("DB_PORT"),
			sslmode="require")
		
		print(colored("PostgreSQL connection is created successfully", "green"))
		return postgre_connection
	except OperationalError as e:
		print(colored(f"Couldn't create the PostgreSQL connection due to exception {e}", "red"))
		return None

def create_table(postgre_connection):
	try:
		cursor = postgre_connection.cursor()
		create_table_query = '''
			CREATE TABLE IF NOT EXISTS spotify_tracks (
			id SERIAL PRIMARY KEY,
			device_name VARCHAR(100),
			device_type VARCHAR(100),
			url VARCHAR(200),
			artists_name TEXT[],
			artists_id TEXT[],
			album_name VARCHAR(100),
			album_picture VARCHAR(200),
			song_name VARCHAR(100),
			song_id VARCHAR(25),
			timestamp VARCHAR(50),
			playing_type VARCHAR(5),
			year INT,
			month INT,
			day INT,
			weekday INT,
			hour INT,
			minute INT
			);
		'''

		cursor.execute(create_table_query)
		postgre_connection.commit()
		print(colored("Table 'spotify_tracks' was created (if it not existed already)", "green"))
	except OperationalError as e:
		print(colored(f"Could not create 'spotify_tracks' due to exception {e}", "red"))
	finally:
		cursor.close()

def write_to_postgres(batch_df, batch_id):
	url = os.getenv("DB_URL")
	user = os.getenv("DB_USER")
	password = os.getenv("DB_PASSWORD")

	if not url or not user or not password:
		print(colored("Database credentials are missing! Please check your environmental variables!", "red"))
		return

	if not batch_df:
		return
	try:
		batch_df.write \
			.format("jdbc") \
			.option("url", url) \
			.option("dbtable", "spotify_tracks") \
			.option("user", user) \
			.option("password", password) \
			.option("driver", "org.postgresql.Driver") \
			.mode("append") \
			.save()
		print(colored(f"Batch {batch_id} loaded to PostgreSQL successfully", "green"))
		return
	except Exception as e:
		print(colored(f"Couldn't load data in the database due to exception: {e}", "red"))
		return

def process_kafka_stream():
	spark_connection = create_spark_connection()
	if not spark_connection:
		return
	
	data_frame = create_kafka_connection(spark_connection)
	if not data_frame:
		return
	
	parsed_df = parse_streaming_schema(data_frame)
	if not parsed_df:
		return
	
	enriched_data = add_datetime_columns(parsed_df)
	if not enriched_data:
		return
	
	postgre_connection = create_postgre_connection()
	if not postgre_connection:
		return
	
	create_table(postgre_connection)
	
	enriched_data.writeStream \
		.outputMode("append") \
		.foreachBatch(write_to_postgres) \
		.start() \
		.awaitTermination()

def send_playback_to_kafka():
	try:
		sp = create_spotify_client()
		if not sp:
			return

		producer = KafkaProducer(bootstrap_servers="localhost:9092",
								value_serializer=lambda v: json.dumps(v).encode("utf-8"))

		redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

		while True:
			try:
				playback = sp.current_playback()
				if playback:
					parsed_playback = parse_playback(playback=playback)
					if parsed_playback and check_duplicates(parsed_playback, redis_client):
						producer.send("spotify_tracks", value=parsed_playback)
						process_kafka_stream()
					else:
						print(colored("Skipping duplicate or invalid track", "cyan"))
				else:
					print(colored("No active playback", "cyan"))
				time.sleep(60)
			except Exception as e:
				print(colored(f"An error occured in the playbsck loop: {e}", "red"))
	except Exception as e:
		print(colored(f"An error occured while sending playback to Kafka: {e}", "red"))

send_playback_to_kafka()
