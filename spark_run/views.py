from django.http.response import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from .models import movieData, ratingData, usersData, zipData
from django.http import HttpResponse
import json
from django.templatetags.static import static
import time
from django.conf import settings
import os
import uuid

def test_spark(request):
    print('Total Records = {}'.format(movieData.count()))
    print('Total Records = {}'.format(usersData.count()))
    print('Total Records = {}'.format(ratingData.count()))
    print('Total Records = {}'.format(zipData.count()))
    ratingData.show()
    return HttpResponse("success")

def table_to_df(table_name):
    if table_name == 'users':
        return usersData
    elif table_name == 'rating':
        return ratingData
    elif table_name == 'zipcodes':
        return zipData
    elif table_name == 'movies':
        return movieData

# def table_to_df_name(table_name):
#     if table_name == 'users':
#         return "usersData"
#     elif table_name == 'rating':
#         return "ratingData"
#     elif table_name == 'zipcodes':
#         return "zipData"
#     elif table_name == 'movies':
#         return "movieData"

table_to_df_name ={'users':'usersData', 'rating': 'ratingData', 'zipcodes': 'zipData', 'movies': 'movieData'}

'''
curl --request POST \
  --url http://127.0.0.1:8000/p_query/ \
  --header 'Content-Type: application/json' \
  --data '{
    "from_table": "users",
    "select_columns": [
        "userid",
        "rating"
    ],
    "join": [
        "inner",
        "users",
        "userid",
        "rating",
        "userid"
    ]
}
'
'''
@csrf_exempt
def p_query(request):
    start_time = time.perf_counter()
    spark_p_query = json.loads(request.body.decode('utf-8'))
    working_dataframe = table_to_df(spark_p_query['from_table'])
    
    if 'join' in spark_p_query:
        if spark_p_query['join'][1] == spark_p_query['from_table']:
            to_join_table = spark_p_query['join'][3]
        elif spark_p_query['join'][3] == spark_p_query['from_table']:
            to_join_table = spark_p_query['join'][1]
        to_join_df = table_to_df(to_join_table)
        join_condition = spark_p_query['join'][2]
        if spark_p_query['join'][0] == 'natural':
            how_to_join = 'inner'
        else:
            how_to_join = spark_p_query['join'][0]
        if join_condition != '':
            working_dataframe = working_dataframe.join(to_join_df,join_condition,how_to_join)
        else:
            raise Exception("join columns not specified")
    where_cond = ' '.join(spark_p_query['where'])
    working_dataframe= working_dataframe.filter(where_cond)
    working_dataframe = working_dataframe.groupBy(spark_p_query['group_by_column'])
    working_dataframe = working_dataframe.agg({spark_p_query['having_condition'][0] : spark_p_query['aggr_function']})
    working_dataframe = working_dataframe.filter(spark_p_query['aggr_function']+'('+spark_p_query['having_condition'][0]+')'+' '+spark_p_query['having_condition'][1]+' '+spark_p_query['having_condition'][2])
    working_dataframe.show()
    filename = uuid.uuid4().hex + '.csv'
    working_dataframe.toPandas().to_csv(os.path.join(settings.BASE_DIR, 'static', filename))
    url = 'http://127.0.0.1:8000' + static(filename)
    total_time_spark = time.perf_counter() - start_time
    return JsonResponse({'spark_time': total_time_spark, 'url': url})
