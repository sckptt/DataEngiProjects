import findspark
import json
import os
import redis
import spotipy
import time
from dotenv import load_dotenv
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, ArrayType, LongType, StructField
from pyspark.sql.functions import from_json, col
from spotipy.oauth2 import SpotifyOAuth

findspark.init()
load_dotenv()

def create_spotify_client():
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
	return my_sp

def parse_playback(playback : dict) -> dict:
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
		return {
			"device_name" : device_name,
			"device_type" : device_type,
			"url" : url,
			"artists_name" : artists_name,
			"artist_id" : artists_id,
			"album_name" : album_name,
			"album_picture" : album_picture,
			"song_name" : song_name,
			"song_id" : song_id,
			"timestamp" : timestamp,
			"playing_type" : playing_type
		}

producer = KafkaProducer(bootstrap_servers="localhost:9092",
						value_serializer=lambda v: json.dumps(v).encode("utf-8"))

my_redis = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

def check_duplicates(pp : dict) -> bool:
	song_id = pp["song_id"]
	timestamp = pp["timestamp"] / 1000
	unique_key = f"{song_id}:{timestamp}"

	if my_redis.exists(unique_key):
		return False
	else:
		my_redis.set(unique_key, "loaded", ex=600)
		return True

#TO_DO - work on log level 
def get_data_from_kafka():
	my_spark = SparkSession.builder \
    	.appName("SpotifyTracks") \
    	.master("local[*]") \
    	.config("spark.driver.extraJavaOptions", "-Duser.library.path=$JAVA_HOME/lib") \
   		.config("spark.executor.extraJavaOptions", "-Duser.library.path=$JAVA_HOME/lib") \
    	.config("spark.java.options", "-Dfile.encoding=UTF-8") \
    	.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
    	.config("spark.ui.enabled", "false") \
    	.getOrCreate()

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

	data_frame = my_spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "spotify_tracks") \
        .option("startingOffsets", "earliest") \
        .load()
    
	parsed_df = data_frame \
        .selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), schema).alias("data")) \
        .select("data.*")
	
	parsed_df.writeStream \
		.outputMode("append") \
		.format("console") \
		.option("truncate", False) \
		.start() 

def send_playback_to_kafka():
	sp = create_spotify_client()
	while True:
		playback = sp.current_playback()
		if playback:
			parsed_playback = parse_playback(playback=playback)
			if parsed_playback and check_duplicates(parsed_playback):
				producer.send("spotify_tracks", value=parsed_playback)
				get_data_from_kafka()
			else:
				print("Skipping duplicate or invalid track")
		else:
			print("No active playback")
		
		time.sleep(120)

send_playback_to_kafka()