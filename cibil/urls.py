from django.urls import path
from .views import dump_page, search_page

app_name = 'cibil'

urlpatterns = [
    path('', dump_page, name='cibil_dump'),
    path('search/', search_page, name='cibil_search'),
]