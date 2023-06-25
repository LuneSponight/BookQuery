from django.db import models

'''
作者表（Author）：
作者ID（AuthorID）：主键，唯一标识每个作者
作者名字（AuthorName）：存储作者的名字

小说表（Novel）：
小说ID（NovelID）：主键，唯一标识每本小说
小说标题（Title）：存储小说的标题
读过的人数（ReaderCount）：存储小说的读者数量
本月点击量（MonthlyClicks）：存储小说在本月的点击量
本月更新时间（UpdateTime）：存储小说本月的最新更新时间
月推荐票数（MonthlyRecommendations）：存储小说本月的推荐票数
介绍（Introduction）：存储小说的简介
总推荐票数（TotalRecommendations）：存储小说的总推荐票数
总粉丝数量（TotalFans）：存储小说的总粉丝数量
礼物数量（GiftCount）：存储小说收到的礼物数量
评论数量（CommentCount）：存储小说的评论数量
书号（BookNumber）：存储小说的书号
/**新增** 字数（WordCount）：存储小说的总字数
作者ID（AuthorID）：外键，关联到作者表中的作者ID
分类ID（CategoryID）：外键，关联到分类表中的分类ID

标签表（Tag）：
标签ID（TagID）：主键，唯一标识每个标签
标签名称（TagName）：存储标签的名称

分类表（Category）：
分类ID（CategoryID）：主键，唯一标识每个分类
分类名称（CategoryName）：存储分类的名称

小说标签关联表（NovelTag）：
关联ID（AssociationID）：主键，唯一标识每个关联记录
小说ID（NovelID）：外键，关联到小说表中的小说ID
标签ID（TagID）：外键，关联到标签表中的标签ID
'''


class Author:
    AuthorID = models.IntegerField(primary_key=True, unique=True)
    AuthorName = models.CharField(max_length=100, default="NOT DEFINE")


class Novel:
    NovelID = models.IntegerField(primary_key=True, unique=True)
    Title = models.CharField(max_length=100, default="NOT DEFINE")
    ReaderCount = models.IntegerField()
    MonthlyClicks = models.IntegerField()
    UpdateTime = models.CharField(max_length=100, default="NONE")
    MonthlyRecommendations = models.IntegerField()
    Introduction = models.CharField(max_length=300, default="NONE")
    TotalRecommendations = models.IntegerField()
    TotalFans = models.IntegerField()
    GiftCount = models.IntegerField()
    CommentCount = models.IntegerField()
    BookNumber = models.IntegerField()
    WordCount = models.IntegerField()
    AuthorID = models.ForeignKey('Author', on_delete=models.CASCADE)
    CategoryID = models.ForeignKey('Category', on_delete=models.CASCADE)


class Tag:
    TagID = models.IntegerField(primary_key=True, unique=True)
    TagName = models.CharField(max_length=20, default="NOT DEFINE")


class Category:
    CategoryID = models.IntegerField(primary_key=True, unique=True)
    CategoryName = models.CharField(max_length=20, default="NOT DEFINE")


class NovelTag:
    AssociationID = models.IntegerField(primary_key=True, unique=True)
    NovelID = models.ForeignKey('Novel', on_delete=models.CASCADE)
    TagID = models.ForeignKey('Tag', on_delete=models.CASCADE)


# Create your models here.
