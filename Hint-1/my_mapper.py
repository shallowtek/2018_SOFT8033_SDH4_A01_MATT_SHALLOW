#!/usr/bin/python

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

#---------------------------------------
#  FUNCTION process_line
#---------------------------------------
def process_line(line):
    # 1. We get rid of the end line character
    line = line.replace('\n', '')

    # 2. We strip any white character at the end
    line = line.rstrip()
    line = line.rstrip('\t')

    # 3. We strip any white character at the beginning
    line = line.strip()
    line = line.strip('\t')

    # 4. We split the info by tabulators or white spaces
    line = line.replace('\t', ' ')
    words = line.split(' ')

    # 5. We get rid of any empty word (if it exists)
    size = len(words)-1
    while size >= 0:
        if words[size] == '':
            del words[size]
        size = size - 1

    # 6. Return the parsed words
    return words


# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, languages, num_top_entries, output_stream):
     
#    I created three lists to store the main variables of the line. 
#    Content is the title, lang is the specific language and page views is the number of page views.
    content_list = []
    page_views_list = []
    lang_list = []
          

    # 2. I loop through input stream and send line to process_line to extract the main variables.
    for text_line in input_stream.readlines():
        # 2.1. create a list of words
        words_list = process_line(text_line)

        # 2.2. I traverse the words 
        for i in range(0, len(words_list)):
            # 2.2.1. I get the first word which is the language
            firstWord = words_list[0]

            # 2.2.2. I get the the first two chars of the first word to check against the languages list
            charOne = firstWord[0]
            charTwo = firstWord[1]
#            I combine the two chars to get the lang I am looking for
            wordLang = charOne + charTwo
#            I get the page views which is the second last element
            page_views = words_list[-2]
            
#            I create a temp list so I can form the main content into one string.
            temp_list = words_list
#           I use the join method to tie all the words together that I want            
            content = "".join(temp_list[1:-2])
            
            # check if the language is in the languages array, if so add to the lists then break to stop adding more.
            if wordLang in languages:                
                lang_list.append(firstWord)
                content_list.append(content)
                page_views_list.append(page_views)
                break;
                
    
    
#   Here I form the new string from all lists and send to output stream.
    for i in range(0, len(page_views_list)): 
        lang = lang_list[i]
        content = content_list[i]
        page_views = page_views_list[i]
        res = lang + '\t' + '(' + content + ',' + page_views + ')' + '\n'
        output_stream.write(res)

    
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, languages, num_top_entries):
    # We pick the working mode:

    # Mode 1: Debug --> We pick a file to read test the program on it
    if debug == True:
        my_input_stream = codecs.open(i_file_name, "r", encoding='utf-8')
        my_output_stream = codecs.open(o_file_name, "w", encoding='utf-8')
    # Mode 2: Actual MapReduce --> We pick std.stdin and std.stdout
    else:
        my_input_stream = sys.stdin
        my_output_stream = sys.stdout

    # We launch the Map program
    my_map(my_input_stream, languages, num_top_entries, my_output_stream)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Input parameters
    debug = True

    i_file_name = "pageviews-20180219-100000_0.txt"
    o_file_name = "mapResult.txt"

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, languages, num_top_entries)
