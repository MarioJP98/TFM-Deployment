import spotipy
from django.conf import settings
from django.http import HttpResponse
from django.shortcuts import render
from spotipy.oauth2 import SpotifyClientCredentials


def index(request):
    return HttpResponse("Hello, world. You're at the recommender index.")


def recommend_view(request):
    recommendation = None

    if request.method == 'POST':
        song_name = request.POST.get('song_name')
        recommendation = f'Recommendations for \"{song_name}\" will be shown here.'

    return render(request, 'recommendations/home_page.html', {'recommendation': recommendation})


def spotify_feature_extractor(request):
    features = None
    error = None
    query = request.GET.get('query', '')

    if query:
        try:
            # Conexión a Spotify
            sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
                client_id=settings.SPOTIFY_CLIENT_ID,
                client_secret=settings.SPOTIFY_CLIENT_SECRET
            ))
            sp._session.verify = False
            # Búsqueda de canción
            results = sp.search(q=query, type='track', limit=1)

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
            else:
                error = "No results found."

        except Exception as e:
            error = str(e)

    return render(request, 'recommendations/spotify_search.html', {
        'query': query,
        'features': features,
        'error': error
    })
