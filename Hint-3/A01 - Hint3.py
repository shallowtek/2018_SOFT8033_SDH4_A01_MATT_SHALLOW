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
    sampleRDD = inputRDD.sample(False,0.01,1234)
    sampleRDD.persist()
    filterRDD = sampleRDD.map(lambda x: x.split(" ")).filter(lambda x: filter_lang(x[0], languages))
    filterRDD.persist()
    sortedRDD = filterRDD.sortBy(lambda x: (x[0],x[2]), False).groupBy(lambda x: x[0]).distinct().mapValues()
    sortedRDD.persist()
    #filterTopRDD = sortedRDD.groupBy(lambda x: x[0]).distinct()
    
    
    for f in sortedRDD.take(20):      
      print(f)

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
