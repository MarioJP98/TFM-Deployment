from spotipy.oauth2 import SpotifyClientCredentials
from django.conf import settings
import spotipy
import requests


class SongNotFoundError(Exception):
    """Excepción personalizada cuando no se encuentra la canción"""
    pass

class SpotifyAPIError(Exception):
    """Excepción personalizada para errores generales de la API de Spotify"""
    pass


def spotify_feature_extractor(song_name):
    features = None

    try:
        # Conexión a Spotify
        sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
            client_id=settings.SPOTIFY_CLIENT_ID,
            client_secret=settings.SPOTIFY_CLIENT_SECRET
        ))
        sp._session.verify = False

        # Búsqueda de canción
        results = sp.search(q=song_name, type='track', limit=1)

        if results['tracks']['items']:
            track = results['tracks']['items'][0]
            track_name = track['name']
            artist_name = track['artists'][0]['name']
            track_id = track['id']

            # Obtener audio features
            audio_features = sp.audio_features(track_id)[0]

            features = {
                'track_name': track_name,
                'artist_name': artist_name,
                'tempo': audio_features['tempo'],
                'loudness': audio_features['loudness'],
                'duration': audio_features['duration_ms'] / 1000,
                'key': audio_features['key'],
                'mode': audio_features['mode'],
                'time_signature': audio_features['time_signature']
            }

            return features
        
        else:
            raise SongNotFoundError("No results found for the song name provided.")

    except (spotipy.exceptions.SpotifyException, requests.exceptions.RequestException) as e:
        # Error de la API de Spotify o de red
        raise SpotifyAPIError(f"Error communicating with Spotify API: {e}")

    except Exception as e:
        # Cualquier otro error inesperado
        raise SpotifyAPIError(f"Unexpected error: {e}")
