# before run the code make sure that there is no directory named result in project folder . if exists please delete that !!


import sys

from pyspark import SparkContext, SparkConf

sc = SparkContext("local", "Word Count")

# part 1
input = sc.textFile("news.txt")
# print(input.collect())


# part 2
test_words = input.map(lambda line: line.split(" "))
# print(words.take(2))


# part 3
words = input.flatMap(lambda line: line.split(" "))
occurrence_words = words.map(lambda word: (word, 1))
# print(occurrence_words.take(10))


# part 4
result = occurrence_words.reduceByKey(lambda a, b: a + b)
# print(result.take(10))

# part 5

# spark default save :
result.saveAsTextFile("result/")


# save results to a seprate text file :
collected = result.collect()

file = open('wordCount.txt', 'w')
for item in collected:
    file.write('('+item[0]+","+str(item[1])+")")
    file.write('\n')
