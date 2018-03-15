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

import codecs
import sys

#---------------------------------------
#  FUNCTION process_line
#---------------------------------------
def process_line(line):
    # 1. We get rid of the end line character
    line = line.replace('\n', '')

    # 2. We strip any white character at the end
    line = line.rstrip()
    line = line.rstrip('\t')

    # 3. We strip any white character at the begining
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
def my_map(input_stream, output_stream):
    
         
    # 2. I loop through input stream and send line to process_line to extract the main variables.
    for text_line in input_stream.readlines():
        # 2.1. create a list of words
        words_list = process_line(text_line)

        # 2.2. I traverse the words 
        for i in range(0, len(words_list)):
            # 2.2.1. I get the first word which is the language
            firstWord = words_list[0]
			
#            I get the page views which is the second last element
            page_views = words_list[-2]

            #page_views_list.append(page_views)
            res = firstWord + '\t' + page_views + '\n'
            output_stream.write(res)
            break;    
    
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name):
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
    my_map(my_input_stream, my_output_stream)

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

    i_file_name = "pageviews-20180219-100000_1.txt"
    o_file_name = "mapResult.txt"

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name)
