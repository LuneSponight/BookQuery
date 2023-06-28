def AddMonthlyClicksKey(x):
    if int(x[1]) < 100:
        return "月点击<100"
    elif int(x[1]) < 1000:
        return "100<月点击<1000"
    elif int(x[1]) < 10000:
        return "1000<月点击<10000"
    else:
        return "月点击>10000"

def AddMonthlyRecommendationsKey(x):
    if int(x[1]) < 100:
        return "月推荐票<100"
    elif int(x[1]) < 1000:
        return "100<月推荐票<1000"
    elif int(x[1]) < 10000:
        return "1000<月推荐票<10000"
    else:
        return "月推荐票>10000"

def AddReaderCountKey(x):
    if int(x[1]) < 1000000:
        return "读者量<一百万"
    elif int(x[1]) < 5000000:
        return "一百万<读者量<五百万"
    elif int(x[1]) < 10000000:
        return "五百万<读者量<一千万"
    elif int(x[1]) < 50000000:
        return "一千万<读者量<五千万"
    elif int(x[1]) < 100000000:
        return "五千万<读者量<一亿"
    else:
        return "读者量>一亿"

def AddWordCountKey(x):
    if int(x[1]) < 1000000:
        return "字数<一百万"
    elif int(x[1]) < 5000000:
        return "一百万<字数<五百万"
    elif int(x[1]) < 10000000:
        return "五百万<字数<一千万"
    elif int(x[1]) < 50000000:
        return "一千万<字数<五千万"
    elif int(x[1]) < 100000000:
        return "五千万<字数<一亿"
    else:
        return "字数>一亿"


def ConvertToValidStr(x):
    return x.replace(",","").split(".")[0]

def AddGiftCountKey(x):
    if int(ConvertToValidStr(x[1])) < 100:
        return "礼物数<100"
    elif int(ConvertToValidStr(x[1])) < 1000:
        return "100<礼物数<1000"
    elif int(ConvertToValidStr(x[1])) < 10000:
        return "1000<礼物数<10000"
    elif int(ConvertToValidStr(x[1])) < 50000:
        return "10000<礼物数<50000"
    elif int(ConvertToValidStr(x[1])) < 100000:
        return "50000<礼物数<100000"
    else:
        return "礼物数>100000"

def AddTotalRecommencdationsKey(x):
    if int(ConvertToValidStr(x[1])) < 10000:
        return "总推荐票<10000"
    elif int(ConvertToValidStr(x[1])) < 50000:
        return "10000<总推荐票<50000"
    elif int(ConvertToValidStr(x[1])) < 100000:
        return "50000<总推荐票<100000"
    elif int(ConvertToValidStr(x[1])) < 500000:
        return "100000<总推荐票<500000"
    elif int(ConvertToValidStr(x[1])) < 1000000:
        return "500000<总推荐票<1000000"
    elif int(ConvertToValidStr(x[1])) < 5000000:
        return "1000000<总推荐票<5000000"
    else:
        return "总推荐票>5000000"

def AddTotalFansKey(x):
    if int(ConvertToValidStr(x[1])) < 500000:
        return "粉丝值<500000"
    elif int(ConvertToValidStr(x[1])) < 1000000:
        return "500000<粉丝值<1000000"
    elif int(ConvertToValidStr(x[1])) < 5000000:
        return "1000000<粉丝值<5000000"
    elif int(ConvertToValidStr(x[1])) < 10000000:
        return "5000000<粉丝值<10000000"
    elif int(ConvertToValidStr(x[1])) < 50000000:
        return "10000000<粉丝值<50000000"
    elif int(ConvertToValidStr(x[1])) < 100000000:
        return "50000000<粉丝值<100000000"
    else:
        return "粉丝值>100000000"

def AddCommentCountKey(x):
    if int(x[1]) < 100:
        return "评论数<100"
    elif int(x[1]) < 500:
        return "100<评论数<500"
    elif int(x[1]) < 1000:
        return "500<评论数<1000"
    elif int(x[1]) < 5000:
        return "1000<评论数<5000"
    elif int(x[1]) < 10000:
        return "5000<评论数<10000"
    else:
        return "评论数>10000"

def JudgeByThree(x,Select_list,Valid_list):
    JudgeByTag = x[0] in Select_list[0]
    JudgeByAuthor = x[1] in Select_list[1]
    JudgeByCategory = x[2] in Select_list[2]
    return (JudgeByTag or (Valid_list[0]) ) and (JudgeByAuthor or (Valid_list[1]) ) and (JudgeByCategory or (Valid_list[2]) )


def CalculateMeanByDiffClass(table):
    return table.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).mapValues(lambda x:round(x[0]/x[1],2))