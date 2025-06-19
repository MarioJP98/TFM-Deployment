from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("recommend/", views.recommend_view, name="recommend_view"),
    path('results/<str:recommendation_id>/', views.get_recommendations),
]