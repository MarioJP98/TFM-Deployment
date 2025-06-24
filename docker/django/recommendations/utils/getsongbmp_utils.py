import requests
from django.conf import settings
import time


class GetSongBPMError(Exception):
    """Excepción personalizada para errores de GetSongBPM"""
    pass


NOTE_TO_INT = {
    "C": 0, "C#": 1, "Db": 1, "D": 2, "D#": 3, "Eb": 3,
    "E": 4, "F": 5, "F#": 6, "Gb": 6, "G": 7, "G#": 8, "Ab": 8,
    "A": 9, "A#": 10, "Bb": 10, "B": 11
}


def parse_time_signature(ts_string):
    """Convierte '4/4' → 4"""
    try:
        return int(ts_string.split('/')[0])
    except:
        return None


def parse_key(key_str):
    """Convierte 'B' → 11, etc."""
    return NOTE_TO_INT.get(key_str.upper(), None)



def getsongbpm_feature_extractor(song_name):
    BASE_URL = "https://api.getsong.co"
    # api_key = settings.GETSONGBPM_API_KEY
    api_key = "49b847750ea5c1e95f54a348099eb988"

    try:
        # 1. Buscar canción
        search_params = {
            "api_key": api_key,
            "type": "song",
            "lookup": song_name,
            "limit": 1,
        }
        response = requests.get(f"{BASE_URL}/search/", params=search_params)
        response.raise_for_status()
        search_data = response.json()

        if not search_data.get("search"):
            raise GetSongBPMError("Song not found in GetSongBPM API.")

        song = search_data["search"][0]
        song_id = song["id"]
        time.sleep(5)
        # 2. Obtener características
        features_response = requests.get(f"{BASE_URL}/song/", params={
            "api_key": api_key,
            "id": song_id
        })
        features_response.raise_for_status()
        data = features_response.json()["song"]

        print({"track_name": data.get("title"),
               "artist_name": data.get("artist", {}).get("name"),
               "tempo": data.get("tempo"),
               "time_signature": parse_time_signature(data.get("time_sig")),
               "key": parse_key(data.get("key_of")),
               "open_key": data.get("open_key"),
               "danceability": data.get("danceability"),
               "acousticness": data.get("acousticness"),
               "album_title": data.get("album", {}).get("title"),
               "release_year": data.get("album", {}).get("year"),
               "genres": data.get("artist", {}).get("genres", [])})

        return {
            "track_name": data.get("title"),
            "artist_name": data.get("artist", {}).get("name"),
            "tempo": data.get("tempo"),
            "time_signature": parse_time_signature(data.get("time_sig")),
            "key": parse_key(data.get("key_of")),
            "open_key": data.get("open_key"),
            "danceability": data.get("danceability"),
            "acousticness": data.get("acousticness"),
            "album_title": data.get("album", {}).get("title"),
            "release_year": data.get("album", {}).get("year"),
            "genres": data.get("artist", {}).get("genres", [])
        }

    except requests.RequestException as e:
        raise GetSongBPMError(f"Request failed: {e}")

    except Exception as e:
        raise GetSongBPMError(f"Unexpected error: {e}")
