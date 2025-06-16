from django.shortcuts import render
from recommendations.utils.kafka_utils import publish_song_features
from recommendations.utils.spotify_utils import (SongNotFoundError,
                                                 SpotifyAPIError,
                                                 spotify_feature_extractor)


def index(request):
    return render(request, "recommendations/index.html")

def recommend_view(request):
    if request.method == "POST":
        song_name = request.POST.get('song_name', '')

        try:
            features = spotify_feature_extractor(song_name)
            publish_song_features(features)
            return render(request, "recommendations/success.html", {"features": features})

        except SongNotFoundError:
            return render(request, "recommendations/error.html", {"error": "Song not found."})

        except SpotifyAPIError as e:
            return render(request, "recommendations/error.html", {"error": f"Spotify API error: {e}"})

        except Exception as e:
            return render(request, "recommendations/error.html", {"error": f"Internal error: {e}"})
    
    else:
        return render(request, "recommendations/error.html", {"error": "Invalid request method."})
