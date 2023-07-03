from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkJDBC").master("spark://master:7077").getOrCreate()
sc = spark.sparkContext
sc.addPyFile('querybook/help.py')

import help

url = "jdbc:mysql://192.168.10.1:3306/bookquery"
properties = {"user": "root", "password": "mysQlSSnig449*"}

Author = spark.read.jdbc(url=url, table="Author", properties=properties)
Novel = spark.read.jdbc(url=url, table="Novel", properties=properties)
Category = spark.read.jdbc(url=url, table="Category", properties=properties)
Tag = spark.read.jdbc(url=url, table="Tag", properties=properties)
NovelTag = spark.read.jdbc(url=url, table="NovelTag", properties=properties)


def CalculateHeatDegree(NovelID_List):
    Top10Heat_Novel_MonthlyClicks = Novel.select(Novel["NovelID"], Novel["Title"], Novel["MonthlyClicks"]).rdd \
        .filter(lambda x: x[0] in NovelID_List).map(lambda x: (x[1], int(x[2]))).sortBy(keyfunc=lambda x: x[1],
                                                                                        ascending=False).take(10)
    RecommendNovelList = []
    RecommendNovelClicksList = []
    for k, v in Top10Heat_Novel_MonthlyClicks:
        RecommendNovelList.append(k)
        RecommendNovelClicksList.append(v)
    return RecommendNovelList, RecommendNovelClicksList


def StatisticsByCount(Params_list):
    Return_Dict = {}
    # 判断筛选条件是否有效
    Valid_list = [Params_list[1] == [], Params_list[2] == [], Params_list[0] == []]

    Select_Category = Params_list[0]
    Select_Category = help.MapIndexToCorCategory(Select_Category)

    Select_Tag = Params_list[1]
    Select_Tag = help.MapIndexToCorTag(Select_Tag)

    Select_Author = Params_list[2]

    Select_CategoryID_list = Category.rdd.map(lambda x: (x[0], x[1])).filter(lambda x: x[1] in Select_Category).map(
        lambda x: x[0]).collect()

    Select_TagID_list = Tag.rdd.filter(lambda x: x[1] in Select_Tag).map(lambda x: x[0]).collect()
    Select_NovelID_list_BasedOnTagID = NovelTag.select(NovelTag["NovelID"], NovelTag["TagID"]).rdd.filter(
        lambda x: x[1] in Select_TagID_list).map(lambda x: x[0]).collect()

    Select_AuthorID_list = Author.rdd.filter(lambda x: x[1] in Select_Author).map(lambda x: x[0]).collect()

    Select_list = [Select_NovelID_list_BasedOnTagID, Select_AuthorID_list, Select_CategoryID_list]

    # 根据三个条件筛选出对应的NovelID列表
    Select_NovelID_list_ByThree = \
        Novel.select(Novel["NovelID"], Novel["AuthorID"], Novel["CategoryID"]) \
            .rdd.filter(lambda x: help.JudgeByThree(x, Select_list, Valid_list)) \
            .map(lambda x: x[0]).collect()

    Select_Novel_Number = len(Select_NovelID_list_ByThree)
    temdict = {}
    temdict["name"] = "总计数量"
    temdict["value"] = Select_Novel_Number
    Return_Dict["总计"] = [temdict]

    # 在选出的小说根据月点击量进行分类并计算每个类别的小说数量
    AddMonthlyClicksKeyDict = help.MakeKeyDict(
        Novel.select(Novel['NovelID'], Novel['MonthlyClicks']).rdd
            .filter(lambda x: x[0] in Select_NovelID_list_ByThree).map(lambda x: int(x[1])).sortBy(
            keyfunc=lambda x: x).collect(), "月点击")
    NumclassifiedbyClicks = Novel.select(Novel['NovelID'], Novel['MonthlyClicks']).rdd.filter(
        lambda x: x[0] in Select_NovelID_list_ByThree).keyBy(
        lambda x: help.AddKey(AddMonthlyClicksKeyDict, x)).countByKey()
    Return_Dict["点击量"] = help.ConvertMyDictToNeedDict(dict(NumclassifiedbyClicks))

    # 在选出的小说根据读者数量进行分类并计算每个类别的小说数量
    AddReaderCountKeyDict = help.MakeKeyDict(
        Novel.select(Novel['NovelID'], Novel['ReaderCount']).rdd
            .filter(lambda x: x[0] in Select_NovelID_list_ByThree).map(lambda x: int(x[1])).sortBy(
            keyfunc=lambda x: x).collect(), "读者量")
    NumclassifiedbyReaderCount = Novel.select(Novel['NovelID'], Novel['ReaderCount']).rdd.filter(
        lambda x: x[0] in Select_NovelID_list_ByThree).keyBy(
        lambda x: help.AddKey(AddReaderCountKeyDict, x)).countByKey()
    Return_Dict["读者数量"] = help.ConvertMyDictToNeedDict(dict(NumclassifiedbyReaderCount))

    # 在选出的小说根据小说字数进行分类并计算每个类别的小说数量
    AddWordCountKeyDict = help.MakeKeyDict(
        Novel.select(Novel['NovelID'], Novel['WordCount']).rdd
            .filter(lambda x: x[0] in Select_NovelID_list_ByThree).map(
            lambda x: int(help.ConvertToValidStr(x[1]))).sortBy(
            keyfunc=lambda x: x).collect(), "字数")
    NumclassifiedbyWordCount = Novel.select(Novel['NovelID'], Novel['WordCount']).rdd.filter(
        lambda x: x[0] in Select_NovelID_list_ByThree).keyBy(lambda x: help.AddKey(AddWordCountKeyDict, x)).countByKey()
    Return_Dict["小说字数"] = help.ConvertMyDictToNeedDict(dict(NumclassifiedbyWordCount))

    # 在选出的小说根据礼物数量进行分类并计算每个类别的小说数量
    AddGiftCountKeyDict = help.MakeKeyDict(
        Novel.select(Novel['NovelID'], Novel['GiftCount']).rdd
            .filter(lambda x: x[0] in Select_NovelID_list_ByThree).map(
            lambda x: int(help.ConvertToValidStr(x[1]))).sortBy(
            keyfunc=lambda x: x).collect(), "礼物数")
    NumclassifiedbyGiftCount = Novel.select(Novel['NovelID'], Novel['GiftCount']).rdd.filter(
        lambda x: x[0] in Select_NovelID_list_ByThree).keyBy(lambda x: help.AddKey(AddGiftCountKeyDict, x)).countByKey()
    Return_Dict["礼物数量"] = help.ConvertMyDictToNeedDict(dict(NumclassifiedbyGiftCount))

    # 在选出的小说根据总推荐票进行分类并计算每个类别的小说数量
    AddTotalRecommendationsKeyDict = help.MakeKeyDict(
        Novel.select(Novel['NovelID'], Novel['TotalRecommendations']).rdd
            .filter(lambda x: x[0] in Select_NovelID_list_ByThree).map(
            lambda x: int(help.ConvertToValidStr(x[1]))).sortBy(
            keyfunc=lambda x: x).collect(), "总推荐票")
    NumclassifiedbyTotalRecoms = Novel.select(Novel['NovelID'], Novel['TotalRecommendations']).rdd.filter(
        lambda x: x[0] in Select_NovelID_list_ByThree).keyBy(
        lambda x: help.AddKey(AddTotalRecommendationsKeyDict, x)).countByKey()
    Return_Dict["总推荐票"] = help.ConvertMyDictToNeedDict(dict(NumclassifiedbyTotalRecoms))

    Return_Dict["barchart_novels"], Return_Dict["barchart_clicks"] = CalculateHeatDegree(Select_NovelID_list_ByThree)

    return Return_Dict


def StatisticsByCategory(Params_list):
    Return_Dict = {}
    # 判断筛选条件是否有效
    Valid_list = [Params_list[1] == [], Params_list[2] == [], Params_list[0] == []]

    Select_Category = Params_list[0]
    Select_Category = help.MapIndexToCorCategory(Select_Category)

    Select_Tag = Params_list[1]
    Select_Tag = help.MapIndexToCorTag(Select_Tag)

    Select_Author = Params_list[2]

    Select_CategoryID_list = Category.rdd.map(lambda x: (x[0], x[1])).filter(lambda x: x[1] in Select_Category).map(
        lambda x: x[0]).collect()

    Select_TagID_list = Tag.rdd.filter(lambda x: x[1] in Select_Tag).map(lambda x: x[0]).collect()
    Select_NovelID_list_BasedOnTagID = NovelTag.select(NovelTag["NovelID"], NovelTag["TagID"]).rdd.filter(
        lambda x: x[1] in Select_TagID_list).map(lambda x: x[0]).collect()

    Select_AuthorID_list = Author.rdd.filter(lambda x: x[1] in Select_Author).map(lambda x: x[0]).collect()

    Select_list = [Select_NovelID_list_BasedOnTagID, Select_AuthorID_list, Select_CategoryID_list]

    # 根据三个条件筛选出对应的NovelID列表
    Select_NovelID_list_ByThree = \
        Novel.select(Novel["NovelID"], Novel["AuthorID"], Novel["CategoryID"]) \
            .rdd.filter(lambda x: help.JudgeByThree(x, Select_list, Valid_list)) \
            .map(lambda x: x[0]).collect()

    # 在选出的小说中计算不同类别小说的数量
    Select_Novel_CateID_NovlID_CateNa = Novel.select(Novel["CategoryID"], Novel["NovelID"]).rdd.filter(
        lambda x: x[1] in Select_NovelID_list_ByThree).join(Category.rdd)
    Return_Dict["不同类别小说的数量"] = help.ConvertMyDictToNeedDict(
        dict(Select_Novel_CateID_NovlID_CateNa.map(lambda x: (x[1][1], x[1][0])).countByKey()))

    # 在选出的小说中计算不同类别小说的平均月点击量
    Select_Novel_CateName_MonthlyClicks = Novel.select(Novel["CategoryID"], Novel["NovelID"],
                                                       Novel["MonthlyClicks"]).rdd.filter(
        lambda x: x[1] in Select_NovelID_list_ByThree) \
        .map(lambda x: (x[0], (x[1], x[2]))).join(Category.rdd).map(lambda x: (x[1][1], int(x[1][0][1])))
    Return_Dict["月点击量"] = help.ConvertMyDictToNeedDict(
        dict(help.CalculateMeanByDiffClass(Select_Novel_CateName_MonthlyClicks).collect()))

    # 在选出的小说中计算不同类别小说的平均读者数量
    Select_Novel_CateName_ReaderCount = Novel.select(Novel["CategoryID"], Novel["NovelID"],
                                                     Novel["ReaderCount"]).rdd.filter(
        lambda x: x[1] in Select_NovelID_list_ByThree) \
        .map(lambda x: (x[0], (x[1], x[2]))).join(Category.rdd).map(lambda x: (x[1][1], int(x[1][0][1])))
    Return_Dict["读者数量"] = help.ConvertMyDictToNeedDict(
        dict(help.CalculateMeanByDiffClass(Select_Novel_CateName_ReaderCount).collect()))

    # 在选出的小说中计算不同类别小说的平均字数
    Select_Novel_CateName_WordCount = Novel.select(Novel["CategoryID"], Novel["NovelID"],
                                                   Novel["WordCount"]).rdd.filter(
        lambda x: x[1] in Select_NovelID_list_ByThree) \
        .map(lambda x: (x[0], (x[1], x[2]))).join(Category.rdd).map(lambda x: (x[1][1], int(x[1][0][1])))
    Return_Dict["小说字数"] = help.ConvertMyDictToNeedDict(
        dict(help.CalculateMeanByDiffClass(Select_Novel_CateName_WordCount).collect()))

    # 在选出的小说中计算不同类别小说的平均礼物数
    Select_Novel_CateName_GiftCount = Novel.select(Novel["CategoryID"], Novel["NovelID"],
                                                   Novel["GiftCount"]).rdd.filter(
        lambda x: x[1] in Select_NovelID_list_ByThree) \
        .map(lambda x: (x[0], (x[1], x[2]))).join(Category.rdd).map(
        lambda x: (x[1][1], int(help.ConvertToValidStr(x[1][0][1]))))
    Return_Dict["礼物数量"] = help.ConvertMyDictToNeedDict(
        dict(help.CalculateMeanByDiffClass(Select_Novel_CateName_GiftCount).collect()))

    # 在选出的小说中计算不同类别小说的平均总推荐票
    Select_Novel_CateName_TotalRecommendations = Novel.select(Novel["CategoryID"], Novel["NovelID"],
                                                              Novel["TotalRecommendations"]).rdd.filter(
        lambda x: x[1] in Select_NovelID_list_ByThree) \
        .map(lambda x: (x[0], (x[1], x[2]))).join(Category.rdd).map(
        lambda x: (x[1][1], int(help.ConvertToValidStr(x[1][0][1]))))
    Return_Dict["总推荐票"] = help.ConvertMyDictToNeedDict(
        dict(help.CalculateMeanByDiffClass(Select_Novel_CateName_TotalRecommendations).collect()))

    Return_Dict["barchart_novels"], Return_Dict["barchart_clicks"] = CalculateHeatDegree(Select_NovelID_list_ByThree)

    return Return_Dict


def StatisticsByTag(Params_list):
    Return_Dict = {}
    # 判断筛选条件是否有效
    Valid_list = [Params_list[1] == [], Params_list[2] == [], Params_list[0] == []]

    Select_Category = Params_list[0]
    Select_Category = help.MapIndexToCorCategory(Select_Category)

    Select_Tag = Params_list[1]
    Select_Tag = help.MapIndexToCorTag(Select_Tag)

    Select_Author = Params_list[2]

    Select_CategoryID_list = Category.rdd.map(lambda x: (x[0], x[1])).filter(lambda x: x[1] in Select_Category).map(
        lambda x: x[0]).collect()

    Select_TagID_list = Tag.rdd.filter(lambda x: x[1] in Select_Tag).map(lambda x: x[0]).collect()
    Select_NovelID_list_BasedOnTagID = NovelTag.select(NovelTag["NovelID"], NovelTag["TagID"]).rdd.filter(
        lambda x: x[1] in Select_TagID_list).map(lambda x: x[0]).collect()

    Select_AuthorID_list = Author.rdd.filter(lambda x: x[1] in Select_Author).map(lambda x: x[0]).collect()

    Select_list = [Select_NovelID_list_BasedOnTagID, Select_AuthorID_list, Select_CategoryID_list]

    # 根据三个条件筛选出对应的NovelID列表
    Select_NovelID_list_ByThree = \
        Novel.select(Novel["NovelID"], Novel["AuthorID"], Novel["CategoryID"]) \
            .rdd.filter(lambda x: help.JudgeByThree(x, Select_list, Valid_list)) \
            .map(lambda x: x[0]).collect()

    # 在选出的小说中中计算不同标签小说的数量
    Select_Novel_TagName_NovelID = NovelTag.select(NovelTag["TagID"], NovelTag["NovelID"]).rdd \
        .filter(lambda x: x[1] in Select_NovelID_list_ByThree).join(Tag.rdd) \
        .map(lambda x: (x[1][1], x[1][0])).filter(lambda x: (x[0] in Select_Tag) or Valid_list[0]).sortBy(
        keyfunc=lambda x: x[1])
    Return_Dict["不同标签小说的数量"] = help.ConvertMyDictToNeedDict(dict(Select_Novel_TagName_NovelID.countByKey()))

    # 在选出的小说中计算不同标签小说的平均月点击量
    Select_Novel_TagName_MonthlyClicks = Select_Novel_TagName_NovelID.map(lambda x: (x[1], x[0])) \
        .join(Novel.select(Novel["NovelID"], Novel["MonthlyClicks"]).rdd).map(lambda x: (x[1][0], int(x[1][1])))
    Return_Dict["月点击量"] = help.ConvertMyDictToNeedDict(
        dict(help.CalculateMeanByDiffClass(Select_Novel_TagName_MonthlyClicks).collect()))

    # 在选出的小说中计算不同标签小说的平均读者数量
    Select_Novel_TagName_ReaderCount = Select_Novel_TagName_NovelID.map(lambda x: (x[1], x[0])) \
        .join(Novel.select(Novel["NovelID"], Novel["ReaderCount"]).rdd).map(lambda x: (x[1][0], int(x[1][1])))
    Return_Dict["读者数量"] = help.ConvertMyDictToNeedDict(
        dict(help.CalculateMeanByDiffClass(Select_Novel_TagName_ReaderCount).collect()))

    # 在选出的小说中计算不同标签小说的平均字数
    Select_Novel_TagName_WordCount = Select_Novel_TagName_NovelID.map(lambda x: (x[1], x[0])) \
        .join(Novel.select(Novel["NovelID"], Novel["WordCount"]).rdd).map(lambda x: (x[1][0], int(x[1][1])))
    Return_Dict["小说字数"] = help.ConvertMyDictToNeedDict(
        dict(help.CalculateMeanByDiffClass(Select_Novel_TagName_WordCount).collect()))

    # 在选出的小说中计算不同标签小说的平均礼物数
    Select_Novel_TagName_GiftCount = Select_Novel_TagName_NovelID.map(lambda x: (x[1], x[0])) \
        .join(Novel.select(Novel["NovelID"], Novel["GiftCount"]).rdd).map(
        lambda x: (x[1][0], int(help.ConvertToValidStr(x[1][1]))))
    Return_Dict["礼物数量"] = help.ConvertMyDictToNeedDict(
        dict(help.CalculateMeanByDiffClass(Select_Novel_TagName_GiftCount).collect()))

    # 在选出的小说中计算不同标签小说的平均总推荐票
    Select_Novel_TagName_TotalRecommendations = Select_Novel_TagName_NovelID.map(lambda x: (x[1], x[0])) \
        .join(Novel.select(Novel["NovelID"], Novel["TotalRecommendations"]).rdd).map(
        lambda x: (x[1][0], int(help.ConvertToValidStr(x[1][1]))))
    Return_Dict["总推荐票"] = help.ConvertMyDictToNeedDict(
        dict(help.CalculateMeanByDiffClass(Select_Novel_TagName_TotalRecommendations).collect()))

    Return_Dict["barchart_novels"], Return_Dict["barchart_clicks"] = CalculateHeatDegree(Select_NovelID_list_ByThree)

    return Return_Dict


def RecommendByAuthorAndNovelName(AuthorList, NovelNameList):
    AuthorID_AuthorName_Dict = dict(Author.rdd.collect())
    CategoryID_CategoryName_Dict = dict(Category.rdd.collect())

    # 推荐相同作者的其他小说
    Select_AuthorIDList = Author.rdd.filter(lambda x: x[1] in AuthorList).map(lambda x: x[0]).collect()
    RecommendNovelListBySameAuthor = Novel.select(
        Novel["CoverLink"], Novel["Title"], Novel["Introduction"], Novel["AuthorID"], Novel["BookNumber"],
        Novel["CategoryID"], Novel["UpdateTime"], Novel["WordCount"]) \
        .rdd.filter(lambda x: x[3] in Select_AuthorIDList and x[1] not in NovelNameList) \
        .map(lambda x: (x[0], x[1], x[2], AuthorID_AuthorName_Dict[x[3]], "https://www.17k.com/book/" + x[4] + ".html",
                        CategoryID_CategoryName_Dict[x[5]], str(x[6]), x[7])).collect()

    # 推荐相同类别的其他小说
    Select_Novel_CategoryIDList = Novel.select(Novel["Title"], Novel["CategoryID"]) \
        .rdd.filter(lambda x: x[0] in NovelNameList).map(lambda x: x[1]).collect()
    RecommendNovelListBySameCategory = Novel.select(
        Novel["CoverLink"], Novel["Title"], Novel["Introduction"], Novel["AuthorID"], Novel["BookNumber"],
        Novel["CategoryID"], Novel["UpdateTime"], Novel["WordCount"], Novel["MonthlyClicks"]) \
        .rdd.filter(lambda x: x[5] in Select_Novel_CategoryIDList and x[1] not in NovelNameList) \
        .map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], int(x[8]))).sortBy(lambda x: x[8],
                                                                                           ascending=False) \
        .map(lambda x: (x[0], x[1], x[2], AuthorID_AuthorName_Dict[x[3]]
                        , "https://www.17k.com/book/" + x[4] + ".html", CategoryID_CategoryName_Dict[x[5]], str(x[6]),
                        x[7])).take(5)

    # 推荐具有相同标签的其他小说
    Select_Novel_NovelIDList = Novel.select(Novel["NovelID"], Novel["Title"]).rdd.filter(
        lambda x: x[1] in NovelNameList) \
        .map(lambda x: x[0]).collect()
    Select_Novel_TagIDList = NovelTag.select(NovelTag["NovelID"], NovelTag["TagID"]).rdd.filter(
        lambda x: x[0] in Select_Novel_NovelIDList) \
        .map(lambda x: x[1]).distinct().collect()

    Recommend_NovelIDList_BySameTag = NovelTag.select(NovelTag["NovelID"], NovelTag["TagID"]).rdd \
        .filter(lambda x: x[1] in Select_Novel_TagIDList).map(lambda x: x[0]).distinct().collect()
    RecommendNovelListBySameTag = Novel.select(
        Novel["CoverLink"], Novel["Title"], Novel["Introduction"], Novel["AuthorID"], Novel["BookNumber"],
        Novel["CategoryID"], Novel["UpdateTime"], Novel["WordCount"], Novel["MonthlyClicks"], Novel["NovelID"]).rdd \
        .filter(lambda x: x[9] in Recommend_NovelIDList_BySameTag and x[1] not in NovelNameList) \
        .map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], int(x[8]))).sortBy(lambda x: x[8],
                                                                                           ascending=False) \
        .map(lambda x: (x[0], x[1], x[2], AuthorID_AuthorName_Dict[x[3]]
                        , "https://www.17k.com/book/" + x[4] + ".html", CategoryID_CategoryName_Dict[x[5]], str(x[6]),
                        x[7])).take(5)

    JsonRecommendNovelListBySameAuthor = help.ConvertRecommendNovelListToJsonFormat(RecommendNovelListBySameAuthor)
    JsonRecommendNovelListBySameCategory = help.ConvertRecommendNovelListToJsonFormat(RecommendNovelListBySameCategory)
    JsonRecommendNovelListBySameTag = help.ConvertRecommendNovelListToJsonFormat(RecommendNovelListBySameTag)

    return help.MakeReturnJsonRecommendNovelListByThree(
        JsonRecommendNovelListBySameAuthor, JsonRecommendNovelListBySameCategory, JsonRecommendNovelListBySameTag)
