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
def process_line(line):
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

    # 4. We append each words to the list
    for word in words:
        if word != '':
            res.append(word)

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION filter languages
# ------------------------------------------
  
def filter_languages(line, languages):
      # 1. We create the output variable
    res = False

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
    
    if "." in first_word:
      lang, project = first_word.split(".", 1)
    else:
      lang = first_word
    
    # 4. We append each words to the list
  
    if lang in languages:
        res = True

    # 5. We return res
    return res
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, languages, num_top_entries):
    inputRDD = sc.textFile(dataset_dir)
    sampleData = inputRDD.sample(False,0.001)
    # 2. We split the dataset by words
    #all_wordsRDD = sampleData.map(lambda x: process_line(x))
    filtered_sample = sampleData.filter(lambda x: filter_languages(x, languages))
    sorted_sample = filtered_sample.sortBy(lambda x: x)
    #filtered_langs = sampleData.filter(lambda x: filter_languages(x, languages))
    #filtered_langs = all_wordsRDD.filter(lambda x: filter_languages(x, languages))    
    for f in sorted_sample.take(20):      
      print(f)

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
