from django.shortcuts import render
from .models import movieData, ratingData, usersData, zipData
from django.http import HttpResponse

def abcd(request):
    print('Total Records = {}'.format(movieData.count()))
    print('Total Records = {}'.format(usersData.count()))
    print('Total Records = {}'.format(ratingData.count()))
    print('Total Records = {}'.format(zipData.count()))
    ratingData.show()
    return HttpResponse("success")

