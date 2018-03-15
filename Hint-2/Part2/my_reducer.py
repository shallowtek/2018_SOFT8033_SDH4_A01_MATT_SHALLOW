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
from operator import itemgetter, attrgetter
#Print to output stream
def print_key_value(lang, value, percentage, output_stream):
           
    res = lang + "\t" + str(value) + "\t" + str(percentage) + "\n"
    output_stream.write(res)

	
def get_key_value(line):
    # 1. Get rid of the end of line at the end of the string
    line = line.replace('\n', '')

    # 2. Split the string by the tabulator delimiter
    words = line.split('\t')

    # 3. Get the key and the value and return them
    lang = words[0]
    value = words[1]

    return value, lang

 
# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(input_stream, total_petitions, output_stream):
    
        
#   I create a number of variables similar to other reducers with addition to an overall list that
#	stores a the total temp views, language and percentage(answer)
    current_lang = ""
    current_page_views = 0
    percentage = 0.0
    temp_total_views = 0
    overall_list = []
    
    # I read the stream and extract the data from each line using get_key_value function
    for text_line in input_stream.readlines():
        #New variables from return of function
        (new_page_views, new_lang) = get_key_value(text_line)
        
        #Similar to other reducers I commented on
        if new_lang != current_lang:          
           if current_lang != "":
		   
			   #Calculate the percentage, round and then append all to an overall list. (nested list)
               percentage = (temp_total_views / total_petitions) * 100
               answer = round(percentage, 2)                             
               overall_list.append((temp_total_views, current_lang, answer))         
           #need to resent the total views for next language/project
           temp_total_views = 0
           current_lang = new_lang
#       Set rest of current variables to new variables. Also add the total views for that project as you loop.                  
        current_page_views = new_page_views
        temp_total_views = temp_total_views + int(current_page_views)
	
	#Need to account for last project or language
    if current_lang != "":
        percentage = (temp_total_views / total_petitions) * 100                     
        overall_list.append((temp_total_views, current_lang, answer))
         
     #sorting the overall list by page views since that will also show highest percentage.
	#Then print so that language is first since I had page views as first item in list
    overall_list.sort(key=itemgetter(0), reverse = True)
    for item in overall_list:       
        print_key_value(item[1], item[0], item[2], output_stream )
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, total_petitions, o_file_name):
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
    my_reduce(my_input_stream, total_petitions, my_output_stream)

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

    # This variable must be computed in the first stage
    total_petitions = 21996631

    i_file_name = "sort_simulation.txt"
    o_file_name = "reduce_simulation.txt"

    my_main(debug, i_file_name, total_petitions, o_file_name)
