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

def get_key_value(line):
    # 1. Get rid of the end of line at the end of the string
    line = line.replace('\n', '')

    # 2. Split the string by the tabulator delimiter
    words = line.split('\t')

    # 3. Get the key and the value and return them
    lang = words[0]
    value = words[1]

    # 4. Get the year and the temperature from value
    value = value.rstrip(')')
    value = value.strip('(')

    fields = value.split(',')
    content = fields[0]
    page_views = fields[-1]

    return page_views, content, lang


#print to output stream
def print_key_value(lang, temp_list, output_stream):

    for content, pageViews in temp_list:       
        res = lang + "\t" + "(" + content + ' , ' + str(pageViews) + ")\t\n"
        output_stream.write(res)

# function used to sort based on value (page views)   
def takeSecond(elem):
    return elem[1]   


# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(input_stream, num_top_entries, output_stream):
    
#   I create three variables to store relevant data.
#   I creat a temp_list to sort the content and pageviews separate to lang
    current_lang = ""
    current_content = ""
    current_page_views = 0
    temp_list = []
    
    
    # I read the stream and extract the data from each line using get_key_value function
    for text_line in input_stream.readlines():
        #New variables from return of function
        (new_page_views, new_content, new_lang) = get_key_value(text_line)
#        I create a new key value pair just for content and page views. 
#       This will be added to my temp list and sorted before sent to output stream
        (k, v) = (current_content, int(current_page_views))
#       Check if the new line has a different lang. If lang is not empty the (k,v) is appended to templist
#       The list is then sorted using a custom function that chooses the second element, I also reverse the sort.
#       So that highest number is top. The print key value function is called where I pass the current lang, first 5
#       parts of temp_list and output stream. I then del that temp_list for next lang.       
        if new_lang != current_lang:          
           if current_lang != "":
               temp_list.append((k,v))
#               sorted(temp_list, key=takeSecond, reverse=True)
               temp_list.sort(key=takeSecond, reverse=True)
               print_key_value(current_lang, temp_list[:5], output_stream)
               del temp_list[:]
               
#           Set current lang to the new lang              
           current_lang = new_lang
#       Set rest of current variables to new variables           
        current_content = new_content
        current_page_views = new_page_views
#       Set the key value so that the first and last line is caught and append to templist
        (k, v) = (current_content, int(current_page_views))
        temp_list.append((k, v))
     
    
#        print_key_value(current_letter, current_num_words, current_total_length, output_stream)   
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, num_top_entries):
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
    my_reduce(my_input_stream, num_top_entries, my_output_stream)

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

    i_file_name = "sort_simulation.txt"
    o_file_name = "reduce_simulation.txt"

    num_top_entries = 5

    my_main(debug, i_file_name, o_file_name, num_top_entries)
