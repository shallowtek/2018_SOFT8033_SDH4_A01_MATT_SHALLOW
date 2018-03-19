#Matt Shallow - 19-Mar-18-18:47
import sys
import codecs

from pyspark.sql.functions import collect_list, udf, explode
from pyspark.sql.types import *
 
def filter_lang(word, languages):
    res = False
    if "." in word:      
      lang, project = word.split(".",1)
    else:
      lang = word
      
    if lang in languages:
      res = True
      
    return res
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, languages, num_top_entries):
    inputRDD = sc.textFile(dataset_dir)
    #sampleRDD = inputRDD.sample(False,0.01,1234)
    inputRDD.persist()
    filterRDD = inputRDD.map(lambda x: x.split(" ")).filter(lambda x: filter_lang(x[0], languages))
    filterRDD.persist()
    sortedRDD = filterRDD.sortBy(lambda x: (x[0],int(x[2])), False)
    sortedRDD.persist()
    combineValueRDD = sortedRDD.map(lambda x: (x[0], x[1] + ",  " + x[2])).toDF(['lang', 'content'])
    #filterTopRDD = sortedRDD.groupBy(lambda x: x[0]).distinct()
    #new = sortedRDD.keyBy(lambda x: x[0]).groupByKey().mapValues()
    
    foo = udf(lambda x:x[0:5], ArrayType(StringType()))
    df_list = (combineValueRDD.groupby('lang').agg(collect_list('content')).
                   withColumn('contents',foo('collect_list(content)')).
                   withColumn('content', explode('contents')).
                   drop('contents', 'collect_list(content)'))
    #df_list.show()
    rdd_list = df_list.rdd.map(list)
    
#     for f in rdd_list.take(20):      
#       print(f)
      
    rdd_list.saveAsTextFile(o_file_dir)
# ------------------------------------------
# MAIN
# ------------------------------------------
if __name__ == '__main__':
    dataset_dir = "/FileStore/tables/A01_my_dataset/"
    o_file_dir = "/FileStore/tables/A01_my_result/"

    languages = ["en", "es", "fr"]
    num_top_entries = 5
    dbutils.fs.rm(o_file_dir, True)
    my_main(dataset_dir, o_file_dir, languages, num_top_entries)
