from django.shortcuts import render
import json
from django.http import JsonResponse


def index(request):
    if request.method == 'GET':
        return render(request, 'index.html')

    if request.method == 'POST':
        json_data = json.load(request.body)
        # json经过一系列处理 #
        return JsonResponse(json_data)
# Create your views here.
