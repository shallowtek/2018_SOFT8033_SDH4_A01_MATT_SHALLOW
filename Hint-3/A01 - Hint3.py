#Matt Shallow - 19-Mar-18-18:47
import sys
import codecs

from pyspark.sql.functions import collect_list, udf, explode
from pyspark.sql.types import *
# ------------------------------------------
# FUNCTION filter_lang
#This function takes in the first word in the line to filter the languages we want.
#The word is spearate by "." if it has one and first part is checked and boolean returned.
# ------------------------------------------ 
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
    #Used a sample for testing
    #sampleRDD = inputRDD.sample(False,0.01,1234)
    #I persist the RDD after each step to speed up processing
    inputRDD.persist()
    #Here I split line into words then filter language using filter_lang function
    filterRDD = inputRDD.map(lambda x: x.split(" ")).filter(lambda x: filter_lang(x[0], languages))
    filterRDD.persist()
    #I sort the data based on language and page views.
    sortedRDD = filterRDD.sortBy(lambda x: (x[0],int(x[2])), False)
    sortedRDD.persist()
    #I convert the RDD to a dataframe so that the top five for each language can be filtered.
    #But before I do this I need to combine both value and content into one string so there will only need to
    #be two columns in the dataframe. There are probably better ways to do this and this is just the way I chose to do it as I wanted to experiment with dataframes.
    combineValueRDD = sortedRDD.map(lambda x: (x[0], x[1] + ",  " + x[2])).toDF(['lang', 'content'])
    
    #Here I create a function called top five and takes in a list grouped by language and slices top five.
    #This new sliced list is then inserted into the column and the old unspliced list is dropped.
    top_five = udf(lambda x:x[0:num_top_entries], ArrayType(StringType()))
    df_list = (combineValueRDD.groupby('lang').agg(collect_list('content')).
                   withColumn('contents',top_five('collect_list(content)')).
                   withColumn('content', explode('contents')).
                   drop('contents', 'collect_list(content)'))
    #df_list.show()
    
    #Convert dataframe back to RDD
    rdd_list = df_list.rdd.map(list)
    
    #Loop through to check all is correct
#     for f in rdd_list.take(20):      
#       print(f)
      
    #Save to file
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
