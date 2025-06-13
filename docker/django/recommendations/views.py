from django.shortcuts import render
from django.http import HttpResponse


def index(request):
    return HttpResponse("Hello, world. You're at the recommender index.")

def recommend_view(request):
    recommendation = None

    if request.method == 'POST':
        song_name = request.POST.get('song_name')
        recommendation = f'Recommendations for \"{song_name}\" will be shown here.'

    return render(request, 'recommendations/home_page.html', {'recommendation': recommendation})