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
# Top Five
# ------------------------------------------
# def top_five(line):
#     current_lang = ""
#     next_lang = line[0]
    
#     temp_list = []
#     overall_list = []    
  
#     if next_lang != current_lang:
      
#       if current_lang != "":
#         temp_list.sort(key = lambda x: x[2])
#         temp_list[:5]
#         overall_list.extend(temp_list)
#         temp_list[:]
          
#       current_lang = next_lang
#       temp_list.append(line)
      
#     overall_list.extend[line]
    
#     return overall_list

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
    
    if "." in first_word:
      lang, project = first_word.split(".", 1)
    else:
      lang = first_word
    
    # 4. We append each words to the list
    
    if lang in languages:
        res.append((first_word, content, page_views))
#         res.append(content)
#         res.append(page_views)
        

    # 5. We return res
    return res
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, languages, num_top_entries):
    inputRDD = sc.textFile(dataset_dir)
    sample_data = inputRDD.sample(False,0.001,1234)
    filter_lang = sample_data.flatMap(lambda x: filter_languages(x, languages)).collect()
    #split_words = sample_data.map(lambda x: x.split(" "))
    #sorted_lang = filter_lang.sortBy(lambda x: (x[0], x[2]))  
    sorted_list = sorted(filter_lang, key=lambda x: (x[0], x[2]), reverse=True)
#   split_words = sorted_lang.map(lambda x: x.split(" "))
    
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
    for f in range(0,50):      
      print(sorted_list[f])

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
