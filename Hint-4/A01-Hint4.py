#Matt Shallow - 19-Mar-18-18:47
import sys
import codecs

from pyspark.sql.functions import collect_list, udf, explode
from pyspark.sql.types import *

accum = sc.accumulator(0)
# ------------------------------------------
# FUNCTION split
# ------------------------------------------
def split(line, per_language_or_project):
  res = []
  
  project = ""
  words = line.split(" ")
  first_word = words[0]
 
  accum.add(long(words[2]))
  
  if "." in first_word:      
      lang, project = first_word.split(".", 1)
  else:
      lang = first_word
      
    
  if per_language_or_project == True:
    res.extend((lang, words[2]))
  else:
    if project != '':
      res.extend((project, words[2]))   
      
  return res

# ------------------------------------------
# FUNCTION percentage
# ------------------------------------------
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, per_language_or_project):
    inputRDD = sc.textFile(dataset_dir)
    
    
    #Used a sample for testing
    #sampleRDD = inputRDD.sample(False,0.01,1234)
    #I persist the RDD after each step to speed up processing
    
    inputRDD.persist()
    #Here I split line into words then filter language using filter_lang function
    filterRDD = inputRDD.map(lambda x: split(x, per_language_or_project)).filter(lambda x: x != [])
    filterRDD.persist()
    #I sort the data based on language and page views.
    sortedRDD = filterRDD.sortBy(lambda x: (x[0],int(x[1])), False)
    sortedRDD.persist()
    
    reduceRDD = sortedRDD.reduceByKey(lambda x,y : int(x)+int(y))
    reduceRDD.persist()
    mapFinalRDD = reduceRDD.map(lambda x: percentage())
#     combineValueRDD = sortedRDD.map(lambda x: (x[0], x[1] + ",  " + x[2])).toDF(['lang', 'content'])
    
#     #Here I create a function called top five that takes in a list grouped by language and slices top five.
#     #This new sliced list is then inserted into the column and the old unspliced list is dropped.
#     top_five = udf(lambda x:x[0:num_top_entries], ArrayType(StringType()))
#     df_list = (combineValueRDD.groupby('lang').agg(collect_list('content')).
#                    withColumn('contents',top_five('collect_list(content)')).
#                    withColumn('content', explode('contents')).
#                    drop('contents', 'collect_list(content)'))
#     #df_list.show()
    
#     #Convert dataframe back to RDD
#     rdd_list = df_list.rdd.map(list)
    print(accum)
    #Loop through to check all is correct
    for f in reduceRDD:      
      print(f)
      
    #Save to file
#     rdd_list.saveAsTextFile(o_file_dir)
# ------------------------------------------
# MAIN
# ------------------------------------------
if __name__ == '__main__':
    dataset_dir = "/FileStore/tables/A01_my_dataset/"
    o_file_dir = "/FileStore/tables/A01_my_result/"

    per_language_or_project = True
    dbutils.fs.rm(o_file_dir, True)
    my_main(dataset_dir, o_file_dir, per_language_or_project)
