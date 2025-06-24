from django.shortcuts import render
from recommendations.utils.kafka_utils import publish_song_features, consume_recommendations_for_id
from recommendations.utils.spotify_utils import (SongNotFoundError,
                                                 SpotifyAPIError,
                                                 spotify_feature_extractor)
from recommendations.utils.getsongbmp_utils import getsongbpm_feature_extractor
from django.http import JsonResponse

def index(request):
    return render(request, "recommendations/index.html")

def recommend_view(request):
    if request.method == "POST":
        song_name = request.POST.get('song_name', '')

        try:
            # Extract fetures from the spotify API
            # features = spotify_feature_extractor(song_name)
            features = getsongbpm_feature_extractor(song_name)
            # features = {
            #     "track_name": "Blinding Lights",
            #     "artist": "The Weeknd",
            #     "tempo": 171.005,
            #     "loudness": -4.999,
            #     "duration": 200.512,
            #     "key": 1,
            #     "mode": 1,
            #     "time_signature": 4,
            #     **{f"timbre_mean_{i}": 0.0 for i in range(12)},
            #     **{f"timbre_std_{i}": 1.0 for i in range(12)}
            # }

            # Get only usable features
            all_features = [
                "track_name","artist_name", "tempo",
                "time_signature","key", "open_key",
                "danceability", "acousticness",
                "album_title", "release_year", "genres"
            ]

            usable_features = [
                "track_name", "artist_name", "tempo",
                "loudness", "duration", "key", "mode",
                "time_signature"
            ] + [f"timbre_mean_{i}" for i in range(12)] + [
                f"timbre_std_{i}" for i in range(12)
            ]

            refactored_features = {
                "track_name": features.get("track_name", ""),
                "artist": features.get("artist_name", ""),
                "tempo": int(features.get("tempo", 0.0)),
                "loudness": int(features.get("loudness", 6)),
                "duration": int(features.get("duration", 200.0)),
                "key": features.get("key", 0) if features.get("key") is not None else 0,
                "mode": features.get("mode", 1),
                "time_signature": features.get("time_signature", 4),
                **{f"timbre_mean_{i}": 0.0 for i in range(12)},
                **{f"timbre_std_{i}": 1.0 for i in range(12)}
            }
            print("Refactored Features:", refactored_features)

            # Publish the features to Kafka
            recommendation_id = publish_song_features(refactored_features)


            return render(request, "recommendations/success.html", {
                "features": refactored_features,
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