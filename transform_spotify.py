import pandas as pd
import json
import click
from datetime import datetime

def load_from_json(path : str, type : int):
	with open(path, "r") as file:
		data = json.load(file)
	if type == 1 or type == 2:
		df_data = pd.DataFrame(data.get('tracks', []))
	elif type == 3:
		df_data = pd.DataFrame(data.get('playlists', []))
	return df_data

def transform_recently_played(df_data : pd.DataFrame):
	df_data["when_played"] = pd.to_datetime(df_data["when_played"])
	df_data["play_year"] = df_data["when_played"].dt.year
	df_data["play_month"] = df_data["when_played"].dt.month
	df_data["play_day"] = df_data["when_played"].dt.day
	df_data["track_number"] = df_data["track_number"].astype(int)
	df_data["track_popularity"] = df_data["track_popularity"].astype(int)
	df_data["transformed"] = datetime.now()
	return df_data

def transform_top_track(df_data : pd.DataFrame):
	df_data["track_number"] = df_data["track_number"].astype(int)
	df_data["track_popularity"] = df_data["track_popularity"].astype(int)
	df_data["transformed"] = datetime.now()
	return df_data

def transform_playlists(df_data : pd.DataFrame):
	df_data["how_many_tracks"] = df_data["how_many_tracks"].astype(int)
	df_data["transformed"] = datetime.now()
	return df_data

def do_the_transformation(df_data : pd.DataFrame, type : int):
	if type == 1:
		transformed_data = transform_recently_played(df_data)
	elif type == 2:
		transformed_data = transform_top_track(df_data)
	elif type == 3:
		transformed_data = transform_playlists(df_data)
	return transformed_data

@click.command()
@click.option('--csv_path', type=str, help='Path to save load batch')
@click.option('--path', type=str, help='Path where JSON is stored')
@click.option('--type_of_data', type=int, help='Which data you want. 1 = recently_played, 2 = top_track, 3 = playlists')
def transform_data(csv_path : str, path : str, type_of_data : int):
	df_data = load_from_json(path, type_of_data)
	transformed_data = do_the_transformation(df_data, type_of_data)
	transformed_data.to_csv(csv_path)

transform_data()