# -*- coding:utf8-*-

import numpy as np

user_data=sc.textFile("/usr/bigdata/data/ml-100k/u.user")

user_fields=user_data.map(lambda line:line.split("|"))

user_occupations=user_fields.map(lambda fields:fields[3])

"""
通过reduceByKey 计算每个职业的数量
"""
count_by_occupations=user_occupations.map(lambda line:(line,1)).reduceByKey(lambda x,y:x+y)

"""
通过countByValue 计算每个职业的数量
注意，此时返回的是defaultdict
"""
count_by_occupations_value=user_occupations.countByValue()



movie_data=sc.textFile("/usr/bigdata/data/ml-100k/u.item")

movie_fields=movie_data.map(lambda line:line.split("|"))

"""
电影时间为空的，则返回1900
"""
def convert_year(x):
    try:
        return int(x[-4:])
    except:
        return 1900

movie_years=movie_fields.map(lambda fields:convert_year(fields[2]))

"""
每年的电影有多少部
"""
count_by_year=movie_years.countByValue()

"""
电影的年龄
从上映日期到1998年的间隔
"""
movie_ages=movie_years.map(lambda age: 1998-age)

"""
电影的年龄分布
"""
count_by_age=movie_ages.countByValue()


rating_data=sc.textFile("/usr/bigdata/data/ml-100k/u.data")

rating_fields=rating_data.map(lambda line:line.split("\t"))
"""
参与评价的用户数量
"""
num_users=rating_fields.map(lambda fields:fields[0]).distinct().count()

"""
所用评级数据
"""
rating_data=rating_fields.map(lambda fields:int(fields[2]))

num_rating=rating_data.count()

total_rating=rating_data.reduce(lambda x,y:x+y)

"""
最高的评级
"""
max_rating=rating_data.reduce(lambda x,y:max(x,y))

min_rating=rating_data.reduce(lambda x,y:min(x,y))

mean_rating=total_rating/num_rating
"""
中位数评级
这个方法不妥  需要collect将数据返回到驱动器
"""
median_rating=np.median(rating_data.collect())

"""
stats函数
返回：
数量count
平均值mean
标准差stdev
最大值max
最小值min
"""
rating_data.stats()


"""
查看每个用户的评级情况
"""
user_rating=rating_fields.map(lambda fields:(int(fields[0]),int(fields[2])))

"""
根据用户id  进行分组
看该用户的评级情况
返回(userId, 评级集合)键值对
"""
user_rating_grouped=user_rating.groupByKey()

"""
看每个用户评级了多少次
返回(userId,评级次数)
"""
user_rating_byuser=user_rating_grouped.map(lambda (k,v):(k,len(v)))