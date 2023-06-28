from django.shortcuts import render
import json
from django.http import JsonResponse, HttpResponse
from pyspark.sql import SparkSession

from booklist import settings

from . import sparkAPI

byCount = 0
byCategory = 1
byTag = 2


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
    db_url = "jdbc:mysql://192.168.10.1:3306/bookquery"

    # 加载数据库数据
    query = "(SELECT NovelID, Title, ReaderCount FROM novel) AS my_query"
    data = spark.read \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", query) \
        .option("user", "root") \
        .option("password", "mysQlSSnig449*") \
        .load()

    # 执行转换操作
    transformed_data = data.filter(data['NovelID'] < 100)
    print(transformed_data)

    # 将转换后的数据转换为Pandas DataFrame
    pandas_df = data.toPandas()

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

    return HttpResponse("Spark processing completed.")


def get_data(request):
    data = {'message': '这是要显示的数据'}
    return JsonResponse(data)


def test(request):
    if request.method == 'GET':
        json_return = {
            "pieChart_Total": [
                {
                    "value": 20,
                    "name": "都市"
                },
                {
                    "value": 30,
                    "name": "修仙"
                },
                {
                    "value": 56,
                    "name": "历史"
                },
                {
                    "value": 12,
                    "name": "玄幻"
                },
                {
                    "value": 22,
                    "name": "现实"
                },
                {
                    "value": 25,
                    "name": "悬疑"
                }
            ],
            "pieChart_Clicks": [
                {
                    "value": 2111,
                    "name": "都市"
                },
                {
                    "value": 3023,
                    "name": "修仙"
                },
                {
                    "value": 1511,
                    "name": "历史"
                },
                {
                    "value": 1233,
                    "name": "玄幻"
                },
                {
                    "value": 2221,
                    "name": "现实"
                },
                {
                    "value": 2578,
                    "name": "悬疑"
                }
            ],
            "pieChart_Recommedations": [
                {
                    "value": 1111,
                    "name": "都市"
                },
                {
                    "value": 723,
                    "name": "修仙"
                },
                {
                    "value": 1511,
                    "name": "历史"
                },
                {
                    "value": 1233,
                    "name": "玄幻"
                },
                {
                    "value": 2721,
                    "name": "现实"
                },
                {
                    "value": 578,
                    "name": "悬疑"
                }
            ],
            "pieChart_WordCount": [
                {
                    "value": 2111100,
                    "name": "都市"
                },
                {
                    "value": 3023000,
                    "name": "修仙"
                },
                {
                    "value": 1511000,
                    "name": "历史"
                },
                {
                    "value": 1233000,
                    "name": "玄幻"
                },
                {
                    "value": 922100,
                    "name": "现实"
                },
                {
                    "value": 957800,
                    "name": "悬疑"
                }
            ],
            "pieChart_GiftCount": [
                {
                    "value": 21110,
                    "name": "都市"
                },
                {
                    "value": 30130,
                    "name": "修仙"
                },
                {
                    "value": 5110,
                    "name": "历史"
                },
                {
                    "value": 12330,
                    "name": "玄幻"
                },
                {
                    "value": 22210,
                    "name": "现实"
                },
                {
                    "value": 9578,
                    "name": "悬疑"
                }
            ],
            "pieChart_ReaderCount": [
                {
                    "value": 211100,
                    "name": "都市"
                },
                {
                    "value": 402300,
                    "name": "修仙"
                },
                {
                    "value": 351100,
                    "name": "历史"
                },
                {
                    "value": 120000,
                    "name": "玄幻"
                },
                {
                    "value": 210000,
                    "name": "现实"
                },
                {
                    "value": 257080,
                    "name": "悬疑"
                }
            ],
            "barChart_novals": ["诛仙", "大道争锋", "升邪", "拔魔", "回到过去变成猫", "赛博时代的魔女", "残袍", "道诡异仙", "诡秘之主", "将夜"],
            "barChart_clicks": [18, 92, 63, 77, 94, 80, 72, 86, 112, 65]
        }
        return render(request, 'test.html', {'json_data': json_return, 'json_str': JsonResponse(json_return).content.decode()})


def analyse_data(request):
    json_return = {}
    print(request.method)
    if request.method == 'POST':
        # convert Request to Json
        print(request.body)
        json_str = request.body.decode('utf-8')
        json_data = json.loads(json_str)

        # read datas from json
        category = json_data['category']
        tags = json_data['tags']
        author = [] if json_data['author'] == '' else [json_data['author']]
        statisticalMethod = json_data['statisticalMethod']

        query_list = [category, tags, author]
        print(query_list)

        # call function from spark
        result = {}
        if statisticalMethod == byCount:
            result = sparkAPI.StatisticsByCount()
        elif statisticalMethod == byCategory:
            result = sparkAPI.StatisticsByCategory()
        elif statisticalMethod == byTag:
            result = sparkAPI.StatisticsByTag()
        else:  # should never happen
            print("Can not identify the query method whose param is statisticsMethod")
        print(result)

        # json_return format
        json_return = {"pieChart_Total": [],
                       "pieChart_Clicks": [],
                       "pieChart_Recommedations": [],
                       "pieChart_WordCount": [],
                       "pieChart_GiftCount": [],
                       "pieChart_ReaderCount": [],
                       "barChart_novals": [],
                       "barChart_clicks": []}

        # json_return = {
        #     "pieChart_Total": [
        #         {
        #             "value": 20,
        #             "name": "都市"
        #         },
        #         {
        #             "value": 30,
        #             "name": "修仙"
        #         },
        #         {
        #             "value": 56,
        #             "name": "历史"
        #         },
        #         {
        #             "value": 12,
        #             "name": "玄幻"
        #         },
        #         {
        #             "value": 22,
        #             "name": "现实"
        #         },
        #         {
        #             "value": 25,
        #             "name": "悬疑"
        #         }
        #     ],
        #     "pieChart_Clicks": [
        #         {
        #             "value": 2111,
        #             "name": "都市"
        #         },
        #         {
        #             "value": 3023,
        #             "name": "修仙"
        #         },
        #         {
        #             "value": 1511,
        #             "name": "历史"
        #         },
        #         {
        #             "value": 1233,
        #             "name": "玄幻"
        #         },
        #         {
        #             "value": 2221,
        #             "name": "现实"
        #         },
        #         {
        #             "value": 2578,
        #             "name": "悬疑"
        #         }
        #     ],
        #     "pieChart_Recommedations": [
        #         {
        #             "value": 1111,
        #             "name": "都市"
        #         },
        #         {
        #             "value": 723,
        #             "name": "修仙"
        #         },
        #         {
        #             "value": 1511,
        #             "name": "历史"
        #         },
        #         {
        #             "value": 1233,
        #             "name": "玄幻"
        #         },
        #         {
        #             "value": 2721,
        #             "name": "现实"
        #         },
        #         {
        #             "value": 578,
        #             "name": "悬疑"
        #         }
        #     ],
        #     "pieChart_WordCount": [
        #         {
        #             "value": 2111100,
        #             "name": "都市"
        #         },
        #         {
        #             "value": 3023000,
        #             "name": "修仙"
        #         },
        #         {
        #             "value": 1511000,
        #             "name": "历史"
        #         },
        #         {
        #             "value": 1233000,
        #             "name": "玄幻"
        #         },
        #         {
        #             "value": 922100,
        #             "name": "现实"
        #         },
        #         {
        #             "value": 957800,
        #             "name": "悬疑"
        #         }
        #     ],
        #     "pieChart_GiftCount": [
        #         {
        #             "value": 21110,
        #             "name": "都市"
        #         },
        #         {
        #             "value": 30130,
        #             "name": "修仙"
        #         },
        #         {
        #             "value": 5110,
        #             "name": "历史"
        #         },
        #         {
        #             "value": 12330,
        #             "name": "玄幻"
        #         },
        #         {
        #             "value": 22210,
        #             "name": "现实"
        #         },
        #         {
        #             "value": 9578,
        #             "name": "悬疑"
        #         }
        #     ],
        #     "pieChart_ReaderCount": [
        #         {
        #             "value": 211100,
        #             "name": "都市"
        #         },
        #         {
        #             "value": 402300,
        #             "name": "修仙"
        #         },
        #         {
        #             "value": 351100,
        #             "name": "历史"
        #         },
        #         {
        #             "value": 120000,
        #             "name": "玄幻"
        #         },
        #         {
        #             "value": 210000,
        #             "name": "现实"
        #         },
        #         {
        #             "value": 257080,
        #             "name": "悬疑"
        #         }
        #     ],
        #     "barChart_novals": ["诛仙", "大道争锋", "升邪", "拔魔", "回到过去变成猫", "赛博时代的魔女", "残袍", "道诡异仙", "诡秘之主", "将夜"],
        #     "barChart_clicks": [18, 92, 63, 77, 94, 80, 72, 86, 112, 65]
        # }

    return JsonResponse(json_return, safe=False)
