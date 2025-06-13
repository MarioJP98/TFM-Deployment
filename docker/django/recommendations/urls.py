from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("recommend/", views.recommend_view, name="recommend_view"),
]