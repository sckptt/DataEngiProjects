import json
import spotipy
import os
import time
import redis
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime, timedelta
from dotenv import load_dotenv
from kafka import KafkaProducer

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
		artists_name = ""
		for i in range(len(playback["item"]["artists"])):
			name = playback["item"]["artists"][i]["name"]
			artists_name += name
			if i + 1 < len(playback["item"]["artists"]):
				artists_name += ", "
		album_name = playback["item"]["album"]["name"]
		album_picture = playback["item"]["album"]["images"][0]["url"]
		song_name = playback["item"]["name"]
		track_id = playback["item"]["id"]
		timestamp = playback["timestamp"]
		playing_type = playback["currently_playing_type"]
		return {
			"device_name" : device_name,
			"device_type" : device_type,
			"url" : url,
			"artists_name" : artists_name,
			"album_name" : album_name,
			"album_picture" : album_picture,
			"song_name" : song_name,
			"track_id" : track_id,
			"timestamp" : timestamp,
			"playing_type" : playing_type
		}

producer = KafkaProducer(bootstrap_servers="localhost:9092",
						value_serializer=lambda v: json.dumps(v).encode("utf-8"))

my_redis = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

def check_duplicates(pp : dict) -> bool:
	track_id = pp["track_id"]
	timestamp = pp["timestamp"] / 1000
	unique_key = f"{track_id}:{timestamp}"

	if my_redis.exists(unique_key):
		return False
	else:
		my_redis.set(unique_key, "loaded", ex=600)
		return True

def get_current_playback():
	sp = create_spotify_client()
	while True:
		playback = sp.current_playback()
		if playback:
			parsed_playback = parse_playback(playback=playback)
			if check_duplicates(parsed_playback):
				producer.send("spotify_tracks", value=parsed_playback)
				print("Track info is sent to Kafka")
		else:
			print("Nothing to send")
		
		time.sleep(120)

get_current_playback()