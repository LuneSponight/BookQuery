def ConvertToValidStr(x):
    return x.replace(",", "").split(".")[0]


def ConvertToBeatifuleNumber(number):
    length = len(str(number))
    if length <= 2:
        dividenumber = number / pow(10, length - 1)
        return int(round(dividenumber, 0) * pow(10, length - 1))
    else:
        dividenumber = number / pow(10, length - 2)
        return int(round(dividenumber, 0) * pow(10, length - 2))


def RandomSelectShowGradient(length):
    if length < 20:
        return [1 / 9, 2 / 9, 5.95 / 9]
    elif length < 100:
        return [2 / 9, 2 / 9, 4.95 / 9]
    elif length < 150:
        return [2 / 9, 3 / 9, 3.95 / 9]
    elif length < 400:
        return [1 / 8, 1 / 8, 3 / 8, 2.95 / 8]
    elif length < 600:
        return [1 / 8, 2 / 8, 2 / 8, 2.95 / 8]
    elif length < 800:
        return [2 / 16, 3 / 16, 5 / 16, 5.95 / 16]
    elif length < 1000:
        return [3 / 16, 4 / 16, 4 / 16, 4.95 / 16]
    elif length < 1400:
        return [1 / 16, 2 / 16, 5 / 16, 7.95 / 16]
    elif length < 1600:
        return [2 / 15, 3 / 15, 3 / 15, 3 / 15, 3.95 / 15]
    elif length < 2000:
        return [1 / 15, 2 / 15, 3 / 15, 4 / 15, 4.95 / 15]
    elif length < 2500:
        return [3 / 25, 4 / 25, 4 / 25, 6 / 25, 7.95 / 25]
    elif length < 3000:
        return [2 / 25, 4 / 25, 5 / 25, 7 / 25, 6.95 / 25]
    elif length < 3500:
        return [3 / 36, 4 / 36, 6 / 36, 7 / 36, 8 / 36, 7.95 / 36]
    elif length < 4000:
        return [2 / 36, 3 / 36, 5 / 36, 7 / 36, 9 / 36, 9.95 / 36]
    else:
        return [4 / 36, 4 / 36, 6 / 36, 7 / 36, 7 / 36, 7.95 / 36]


def MakeKeyDict(ZhibiaoList, ZhiBiaoName):
    Gradients = RandomSelectShowGradient(len(ZhibiaoList))
    AccumuldateGradient = 0
    SplitDatasList = []
    Length = len(ZhibiaoList)
    for gradient in Gradients:
        AccumuldateGradient += gradient
        if ZhibiaoList[int(Length * AccumuldateGradient)] not in SplitDatasList:
            SplitDatasList.append(ZhibiaoList[int(Length * AccumuldateGradient)])
    SplitDatasList[len(SplitDatasList) - 1] = ZhibiaoList[len(ZhibiaoList) - 1] + 1
    AddKeyDict = {}
    for i in range(len(SplitDatasList)):
        if i == 0:
            AddKeyDict[SplitDatasList[i]] = ZhiBiaoName + "<=" + str(ConvertToBeatifuleNumber(SplitDatasList[i]))
        else:
            AddKeyDict[SplitDatasList[i]] = str(ConvertToBeatifuleNumber(SplitDatasList[i - 1])) \
                                            + "<" + ZhiBiaoName + "<=" + str(
                ConvertToBeatifuleNumber(SplitDatasList[i]))
    return AddKeyDict


def AddKey(AddKeyDict, x):
    for key, value in AddKeyDict.items():
        if int(ConvertToValidStr(x[1])) <= key:
            return value


def JudgeByThree(x, Select_list, Valid_list):
    JudgeByTag = x[0] in Select_list[0]
    JudgeByAuthor = x[1] in Select_list[1]
    JudgeByCategory = x[2] in Select_list[2]
    return (JudgeByTag or (Valid_list[0])) and (JudgeByAuthor or (Valid_list[1])) and (
                JudgeByCategory or (Valid_list[2]))


def CalculateMeanByDiffClass(table):
    return table.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).mapValues(
        lambda x: round(x[0] / x[1], 2))


def MapIndexToCorCategory(CategoryIndexList):
    Index_CategoryName_Refer = {"0": "东方玄幻", "1": "都市激战", "2": "异界大陆", "3": "时空快穿", "4": "历史穿越",
                                "5": "古典仙侠",
                                "6": "蜜爱甜宠", "7": "都市异能", "8": "末世危机", "9": "女尊女强", "10": "都市生活",
                                "11": "架空历史",
                                "12": "兽世王朝", "13": "总裁豪门", "14": "都市重生", "15": "游戏生涯",
                                "16": "奇幻修真",
                                "17": "年代种田", "18": "虚拟网游", "19": "空间种田"}
    Tem_Select_Category = []
    for data in CategoryIndexList:
        Tem_Select_Category.append(Index_CategoryName_Refer[str(data)])
    return Tem_Select_Category


def MapIndexToCorTag(TagIndexList):
    Index_TagName_Refer = {"0": "热血", "1": "爽文", "2": "穿越", "3": "重生", "4": "玄幻", "5": "搞笑", "6": "系统",
                           "7": "甜宠", "8": "都市", "9": "升级", "10": "美女", "11": "总裁", "12": "复仇",
                           "13": "种田",
                           "14": "扮猪吃虎", "15": "腹黑", "16": "虐恋", "17": "争霸", "18": "爱情", "19": "女强"}
    Tem_Select_Tag = []
    for data in TagIndexList:
        Tem_Select_Tag.append(Index_TagName_Refer[str(data)])
    return Tem_Select_Tag


def ConvertMyDictToNeedDict(dic):
    Return_List = []
    for key, value in dic.items():
        temdict = {}
        temdict["value"] = value
        temdict["name"] = key
        Return_List.append(temdict)
    return Return_List
