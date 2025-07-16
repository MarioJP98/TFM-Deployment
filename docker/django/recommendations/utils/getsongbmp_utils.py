import requests
from django.conf import settings
import time


class GetSongBPMError(Exception):
    """Excepción personalizada para errores de GetSongBPM"""
    pass


NOTE_TO_KEY = {
    "C": 0, "C#": 1, "Db": 1,
    "D": 2, "D#": 3, "Eb": 3,
    "E": 4,"F": 5, "F#": 6,
    "Gb": 6,"G": 7, "G#": 8,
    "Ab": 8,"A": 9, "A#": 10,
    "Bb": 10,"B": 11
}

def extract_key_mode(key_of):
    key_of = key_of.strip().replace("♯", "#").replace("♭", "b")
    if key_of[-1].lower() == 'm':
        mode = 0
        root = key_of[:-1]
    else:
        mode = 1
        root = key_of
    key = NOTE_TO_KEY.get(root, None)
    return key, mode


def estimate_loudness_from_acousticness(acousticness):
    """
    Estima loudness en escala [-60, 0] a partir de acousticness [0,100]
    """
    return round((1 - acousticness / 100) * 60 - 60, 2)



def parse_time_signature(ts_string):
    """Convierte '4/4' → 4"""
    try:
        return int(ts_string.split('/')[0])
    except:
        return None



def refactor_getsongbpm_features(features_raw):
    """
    Transforma las características brutas de la API GetSongBPM
    al formato esperado por el modelo de recomendación.
    """
    key_str = features_raw.get("key", "C")
    key, mode = extract_key_mode(key_str)

    time_signature = parse_time_signature(
        features_raw.get("time_signature", "4/4"))

    loudness = estimate_loudness_from_acousticness(
        float(features_raw.get("acousticness", 50)))

    return {
        "track_name": features_raw.get("title", ""),
        "artist_name": features_raw.get("artist_name", ""),
        "tempo": float(features_raw.get("tempo", 0.0)),
        "loudness": loudness,
        "key": key if key is not None else 0,
        "mode": mode if mode is not None else 1,
        "time_signature": time_signature if time_signature is not None else 4,
    }


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

        return refactor_getsongbpm_features(data)

    except requests.RequestException as e:
        raise GetSongBPMError(f"Request failed: {e}")

    except Exception as e:
        raise GetSongBPMError(f"Unexpected error: {e}")
