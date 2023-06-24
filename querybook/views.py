from django.shortcuts import render

def hello(request):
    return render(request, 'index.html')
# Create your views here.
