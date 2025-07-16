from django.shortcuts import render
from recommendations.utils.kafka_utils import publish_song_features, consume_recommendations_for_id
from recommendations.utils.spotify_utils import (SongNotFoundError,
                                                 SpotifyAPIError,
                                                 spotify_feature_extractor)
from recommendations.utils.getsongbmp_utils import getsongbpm_feature_extractor
from django.http import JsonResponse
import json

def index(request):
    return render(request, "recommendations/index.html")

def recommend_view(request):
    if request.method == "POST":
        song_name = request.POST.get('song_name', '')
        artist_name = request.POST.get("artist_name", "")


        try:
            # Extract fetures from the spotify API
            # features = spotify_feature_extractor(song_name)

            features = getsongbpm_feature_extractor(song_name, artist_name)
            browser_context = request.session.get("browser_context", {})

            print("Refactored Features:", features)

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
            print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
            return render(request, "recommendations/error.html", {"error": f"Internal error: {e}"})

    else:
        return render(request, "recommendations/error.html", {"error": "Invalid request method."})

def get_recommendations(request, recommendation_id):
    recommendations = consume_recommendations_for_id(recommendation_id)
    return JsonResponse({"recommendations": recommendations})

def capture_context(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            request.session['browser_context'] = data  # Guardamos en sesión
            return JsonResponse({"status": "ok"})
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=400)
    return JsonResponse({"error": "Invalid method"}, status=405)
