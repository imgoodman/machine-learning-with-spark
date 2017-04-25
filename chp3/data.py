# -*- coding:utf8-*-

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