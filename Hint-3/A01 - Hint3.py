# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import sys
import codecs

from pyspark.sql.functions import collect_list, udf, explode
from pyspark.sql.types import *

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------

# def process_line(line):
  
#     res = []
#     # 2. We clean the line

#     line = line.replace('\t', ' ')
#     line = line.replace('  ', ' ')
#     line = line.replace('   ', ' ')
#     line = line.replace('    ', ' ')
#     line = line.strip()
#     line = line.rstrip()

#     # 3. We split the line by words
#     words = line.split(" ")
    
#     # 4. We append each words to the list
#     for word in words:
#         if word != '':
#             res.append(word)

#     # 5. We return res
#     return res




# ------------------------------------------
# FUNCTION filter languages
# ------------------------------------------
  
def filter_languages(line, languages):
      # 1. We create the output variable
    res = []
    
    # 2. We clean the line
    line = line.replace('\t', ' ')
    line = line.replace('  ', ' ')
    line = line.replace('   ', ' ')
    line = line.replace('    ', ' ')

    line = line.strip()
    line = line.rstrip()

    # 3. We split the line by words
    words = line.split(" ")
    first_word = words[0]
    content = words[1]
    page_views = words[2]
    combined = first_word + " " + content + " " + page_views 
    
    if "." in first_word:
      lang, project = first_word.split(".", 1)
    else:
      lang = first_word
    
    # 4. We append each words to the list
    
    if lang in languages:
   
      res.append([first_word, content, page_views])
  #         res.append(content)
  #         res.append(page_views)
        
    # 5. We return res
    return res
  
  # ------------------------------------------
# Top Five
# ------------------------------------------
# def top_five(lineX, lineY):
  
    
#     overall_list = []
#     temp_list = []
#     temp_list.append(" ")
#     last_lang = temp_list[-1]
#     next_lang = lineY[0]
#     content = lineY[1]
#     views = lineY[2]
#     combined = next_lang + " " + content + " " + views 
    

      
#     if next_lang != last_lang:
      
#       if last_lang != " ":
#         temp_list[:5]
#         overall_list.append(temp_list)
#         del temp_list[:]
        
    
#     temp_list.append(combined)
    
      
    
#     return overall_list
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, languages, num_top_entries):
    inputRDD = sc.textFile(dataset_dir)
    sample_data = inputRDD.sample(False,0.01,1234)
    sample_data.persist()
    filter_lang = sample_data.flatMap(lambda x: filter_languages(x, languages))
    #split_words = sample_data.map(lambda x: x.split(" "))
    filter_lang.persist()
    sorted_lang = filter_lang.sortBy(lambda x: (x[0], x[2]), False)
    sorted_lang.persist()
#     filter_top_five = sorted_lang.flatMap(lambda x: top_five(x))
    df = sorted_lang.toDF(["lang", "content"])
  
    foo = udf(lambda x:x[0:5], ArrayType(StringType()))
    df_list = (df.groupby('lang').agg(collect_list('content')).
                   withColumn('values',foo('collect_list(content)')).
                   withColumn('content', explode('values')).
                   drop('values', 'collect_list(content)'))
    df_list.show()
    
#     sorted_lang.persist()
    #filter_top_five = sorted_lang.flatMap(lambda x: top_five(x))
#     sorted_list = sorted(filter_lang, key=lambda x: (x[0], x[2]), reverse=True)
    
    #L = [list(v) for k,v in groupby(sorted_list)] 
    #filtered_views = sorted_list.flatMap(lambda x: top_five(x)).collect()
      
   #filter_views = split_words.flatMap(lambda x: top_five(x))
    #sorted_views = split_words.sortBy(lambda x: x[2], False)
    # 2. We split the dataset by words
#     all_wordsRDD = sorted_lang.map(lambda x: top_five(x))
    #filtered_sample = sampleData.filter(lambda x: filter_languages(x, languages))
    #sorted_sample = filtered_sample.sortBy(lambda x: x)
    #words_sample = sorted_sample.map(lambda x: x.split(" "))
   #sorted_sample_two = words_sample.sortBy(lambda x: x[2], False)
    #filtered_langs = sampleData.filter(lambda x: filter_languages(x, languages))
    #filtered_langs = all_wordsRDD.filter(lambda x: filter_languages(x, languages))    
#     for f in filter_top_five.take(20):      
#       print(f)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    dataset_dir = "/FileStore/tables/A01_my_dataset/"
    o_file_dir = "/FileStore/tables/A01_my_result/"

    languages = ["en", "es", "fr"]
    num_top_entries = 5
    dbutils.fs.rm(o_file_dir, True)
    my_main(dataset_dir, o_file_dir, languages, num_top_entries)
