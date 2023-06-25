from django.shortcuts import render
import json
from django.http import JsonResponse, HttpResponse
from pyspark.sql import SparkSession

from booklist import settings


def index(request):
    if request.method == 'GET':
        return render(request, 'index.html')

    if request.method == 'POST':
        json_data = json.load(request.body)
        # json经过一系列处理 #
        return JsonResponse(json_data)


# 示例
def call_spark_interface(request):
    # 创建SparkSession
    spark = SparkSession.builder \
        .appName("Django Spark Integration") \
        .getOrCreate()

    # 数据库连接配置
    db_url = "jdbc:mysql://localhost:3306/your_database"
    db_properties = {
        "user": "your_username",
        "password": "your_password",
        "driver": "com.mysql.jdbc.Driver"
    }

    # 加载数据库数据
    query = "(SELECT column1, column2 FROM your_table) AS your_query"
    data = spark.read \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", query) \
        .option("properties", db_properties) \
        .load()

    # 执行转换操作
    transformed_data = data.filter(data['column1'] > 10)

    # 将转换后的数据转换为Pandas DataFrame
    pandas_df = transformed_data.toPandas()

    # 将转换后的数据作为JSON响应返回
    result = {
        'data': pandas_df.to_dict(orient='records')
    }
    return JsonResponse(result)


def my_view(request):
    # 创建SparkSession对象
    spark = SparkSession.builder \
        .appName(settings.SPARK_APP_NAME) \
        .master(settings.SPARK_MASTER) \
        .config('spark.executor.memory', settings.SPARK_EXECUTOR_MEMORY) \
        .getOrCreate()

    # 使用Spark进行数据处理
    # ...

    return HttpResponse("Spark processing completed.")


def get_data(request):
    data = {'message': '这是要显示的数据'}
    return JsonResponse(data)
