import click
import json
import spotipy
import os
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv

load_dotenv()

def create_spotify_client():
	my_id = os.getenv("SPOTIPY_CLIENT_ID")
	my_secret= os.getenv("SPOTIPY_CLIENT_SECRET")
	my_scopes = "playlist-read-private user-top-read user-read-recently-played"
	my_sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
		client_id=my_id,
		client_secret=my_secret,
		redirect_uri=os.getenv("SPOTIPY_REDIRECT_URI"),
		scope=my_scopes))
	return my_sp

def parse_json_top_tracks(top_tracks : dict, i :int) -> dict:
	track_id = top_tracks["items"][i]["id"]
	artist_name = top_tracks["items"][i]["artists"][0]["name"]
	album_name = top_tracks["items"][i]["album"]["name"]
	release_date = top_tracks["items"][i]["album"]["release_date"]
	song_name = top_tracks["items"][i]["name"]
	track_number = top_tracks["items"][i]["track_number"]
	track_popularity = top_tracks["items"][i]["popularity"]
	external_url = top_tracks["items"][i]["external_urls"]["spotify"]
	return {"track_id" : track_id,
		"artist_name" : artist_name,
		"album_name" : album_name,
		"release_date" : release_date,
		"song_name" : song_name,
		"track_number" : track_number,
		"track_popularity" : track_popularity,
		"external_url" : external_url}

def parse_json_recently_played(recently_played : dict, i :int) -> dict:
	track_id = recently_played["items"][i]["track"]["id"]
	artist_name = recently_played["items"][i]["track"]["artists"][0]["name"]
	album_name = recently_played["items"][i]["track"]["album"]["name"]
	release_date = recently_played["items"][i]["track"]["album"]["release_date"]
	song_name = recently_played["items"][i]["track"]["name"]
	when_played = recently_played["items"][i]["played_at"]
	track_number = recently_played["items"][i]["track"]["track_number"]
	track_popularity = recently_played["items"][i]["track"]["popularity"]
	external_url = recently_played["items"][i]["track"]["external_urls"]["spotify"]
	return {"track_id" : track_id,
		"artist_name" : artist_name,
        "album_name" : album_name,
        "release_date" : release_date,
        "song_name" : song_name,
        "when_played" : when_played,
        "track_number" : track_number,
        "track_popularity" : track_popularity,
        "external_url" : external_url}

def parse_json_playlists(playlists : dict, i :int) -> dict:
	playlist_id = playlists["items"][i]["id"]
	playlist_name = playlists["items"][i]["name"]
	playlist_description = playlists["items"][i]["description"]
	playlist_owner = playlists["items"][i]["owner"]["display_name"]
	external_urls = playlists["items"][i]["owner"]["external_urls"]["spotify"]
	how_many_tracks = playlists["items"][i]["tracks"]["total"]
	return{"playlist_id" : playlist_id,
		"playlist_name" : playlist_name,
		"playlist_description" : playlist_description,
		"playlist_owner" : playlist_owner,
		"external_urls" : external_urls,
		"how_many_tracks" : how_many_tracks}

def get_recently_played(dict_for_parsing : dict, i : int, data_type : int) -> dict:
	if data_type == 1:
		parsed_list = parse_json_recently_played(dict_for_parsing, i)
	elif data_type == 2:
		parsed_list = parse_json_top_tracks(dict_for_parsing, i)
	elif data_type == 3:
		parsed_list = parse_json_playlists(dict_for_parsing, i)
	return parsed_list

def load_data(result_path : str, dict_for_parsing : dict, limits : int, data_type : int):
	if data_type == 1:
		print("Start getting info about your recently played tracks!")
	if data_type == 2:
		print("Start getting info about your top tracks!")
	if data_type == 3:
		print("Start getting info about your playlists!")
	
	list = []
	total_items = len(dict_for_parsing["items"])

	for i in range(min(total_items, limits)):
		parsed_top_track = get_recently_played(dict_for_parsing, i, data_type)
		list.append(parsed_top_track)
	
	if data_type == 1 or data_type == 2:
		our_data = {"number_of_tracks" : min(total_items, limits),
			"tracks" : list}
	elif data_type == 3:
		our_data = {"number_of_playlists" : min(total_items, limits),
			"playlists" : list}
	print("Saving your data in file, please wait...")

	with open(result_path, "w") as file:
		json.dump(our_data, file, indent=2, ensure_ascii=False)

	print("Job is done!")

@click.command()
@click.option('--result_path', type=str, help='Path to save your data')
@click.option('--type_of_data', type=int, help='Which data you want. 1 = recently_played, 2 = top_track, 3 = playlists')
@click.option('--limits', type=int, help='How many tracks you want to get')
def load_songs_cli(result_path : str, type_of_data : int, limits : int):
	sp = create_spotify_client()
	if type_of_data == 1:
		recently_played = sp.current_user_recently_played(limit=limits)
		load_data(result_path, recently_played, limits, type_of_data)
	elif type_of_data == 2:
		top_tracks = sp.current_user_top_tracks(limit=limits, time_range="medium_term")
		load_data(result_path, top_tracks, limits, type_of_data)
	elif type_of_data == 3:
		playlists = sp.current_user_playlists(limit=limits)
		load_data(result_path, playlists, limits, type_of_data)

load_songs_cli()