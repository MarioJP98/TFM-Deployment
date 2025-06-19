from django.shortcuts import render
from recommendations.utils.kafka_utils import publish_song_features, consume_recommendations_for_id
from recommendations.utils.spotify_utils import (SongNotFoundError,
                                                 SpotifyAPIError,
                                                 spotify_feature_extractor)
from django.http import JsonResponse

def index(request):
    return render(request, "recommendations/index.html")

def recommend_view(request):
    if request.method == "POST":
        song_name = request.POST.get('song_name', '')

        try:
            # Extract fetures from the spotify API
            # features = spotify_feature_extractor(song_name)
            features = {
                "track_name": "Blinding Lights",
                "artist": "The Weeknd",
                "tempo": 171.005,
                "loudness": -4.999,
                "duration": 200.512,
                "key": 1,
                "mode": 1,
                "time_signature": 4,
                **{f"timbre_mean_{i}": 0.0 for i in range(12)},
                **{f"timbre_std_{i}": 1.0 for i in range(12)}
            }

            # Publish the features to Kafka
            recommendation_id = publish_song_features(features)
            return render(request, "recommendations/success.html", {
                "features": features,
                "recommendation_id": str(recommendation_id, 'utf-8')
            })

        except SongNotFoundError:
            return render(request, "recommendations/error.html", {"error": "Song not found."})

        except SpotifyAPIError as e:
            return render(request, "recommendations/error.html", {"error": f"Spotify API error: {e}"})

        except Exception as e:
            return render(request, "recommendations/error.html", {"error": f"Internal error: {e}"})
    
    else:
        return render(request, "recommendations/error.html", {"error": "Invalid request method."})

def get_recommendations(request, recommendation_id):
    recommendations = consume_recommendations_for_id(recommendation_id)
    return JsonResponse({"recommendations": recommendations})