import json
import os
import redis
import spotipy
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
from kafka import KafkaProducer
from spotipy.oauth2 import SpotifyOAuth
from termcolor import colored

load_dotenv()

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

def send_playback_to_kafka():
	try:
		sp = create_spotify_client()
		if not sp:
			return

		producer = KafkaProducer(bootstrap_servers="localhost:9092",
								value_serializer=lambda v: json.dumps(v).encode("utf-8"))

		redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

		playback = sp.current_playback()
		if playback:
			parsed_playback = parse_playback(playback=playback)
			if parsed_playback and check_duplicates(pp=parsed_playback, redis_client=redis_client):
				producer.send("spotify_tracks", value=parsed_playback)
			else:
				print(colored("Skipping duplicate or invalid track", "cyan"))
		else:
			print(colored("No active playback", "cyan"))
	except Exception as e:
		print(colored(f"An error occured while sending playback to Kafka: {e}", "red"))

default_args = {
	'owner': 'sckptt', 
	'depends_on_past': False, 
	'start_date': datetime(2025, 2, 17, 10, 00), 
	'retries': 1,
	'retry_delay' : timedelta(seconds=30)
}

with DAG(
	'send_playback_to_kafka', 
	description='Get current playback from Spotify API, parse it and send it to Kafka',
	default_args=default_args, 
	schedule='* * * * *', 
	catchup=False
	) as dag:
	task = PythonOperator(
		task_id = 'stream_data_from_spotify_API',
		python_callable=send_playback_to_kafka)
