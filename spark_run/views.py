from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from .models import movieData, ratingData, usersData, zipData
from django.http import HttpResponse
import json

def test_spark(request):
    print('Total Records = {}'.format(movieData.count()))
    print('Total Records = {}'.format(usersData.count()))
    print('Total Records = {}'.format(ratingData.count()))
    print('Total Records = {}'.format(zipData.count()))
    ratingData.show()
    return HttpResponse("success")

@csrf_exempt
def p_query(request):
    spark_p_query = json.loads(request.body.decode('utf-8'))
    if spark_p_query['from'] == 'users':
        working_dataframe = usersData
    elif spark_p_query['from'] == 'rating':
        working_dataframe = ratingData
    elif spark_p_query['from'] == 'zipcodes':
        working_dataframe = zipData
    elif spark_p_query['from'] == 'movies':
        working_dataframe = movieData
    