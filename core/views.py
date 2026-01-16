from django.shortcuts import render
from django.utils import timezone

def home(request):
    context = {
        'now': timezone.now(),
    }
    return render(request, "core/home.html", context)