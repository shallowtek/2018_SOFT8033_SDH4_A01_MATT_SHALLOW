#Matt Shallow - 22-Mar-18-02:16
import sys
import codecs


accum = sc.accumulator(0)

#This function splits the line and checks whether it is a project or lang using boolean.
#Then returns either project or lang list for that line
# ------------------------------------------
# FUNCTION split
# ------------------------------------------
def split(line, per_language_or_project):
  res = []
  
  project = ""
  words = line.split(" ")
  accum.add(int(words[2]))
  
  first_word = words[0]
  
  if "." in first_word:      
      lang, project = first_word.split(".", 1)
  else:
      lang = first_word
      
    
  if per_language_or_project == True:
    res.extend((lang, words[2]))
  else:
    if project != '':
      res.extend((project, words[2]))   
      
  return res

#This function calculates the percentage. It takes in a line and the total_partitions
#I cast values to floats and then use round. New mapping is returned.
# ------------------------------------------
# FUNCTION get_percentage
# ------------------------------------------
def get_percentage(x, total_petitions):
  res = []   
  percentage = float((float(x[1])/float(total_petitions)) * 100)
  #answer = round(percentage, 2) 
  res.extend((x[0], x[1], str(percentage) + "%"))
  return res
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, per_language_or_project):
    inputRDD = sc.textFile(dataset_dir)
    
    #I persist the RDD after each step to speed up processing
    inputRDD.persist()
    #I create a function split that splits line into words, checks project or language boolean and returns a new mapping.
    #I then filter any blank projects or langs
    filterRDD = inputRDD.map(lambda x: split(x, per_language_or_project)).filter(lambda x: x != [])
    filterRDD.persist()
    #I reduce the data by key so that the page views are summed together  
    reduceRDD = filterRDD.reduceByKey(lambda x,y : int(x)+int(y))
    reduceRDD.persist()
    #I then sort based on page views
    sortedRDD = reduceRDD.sortBy(lambda x: int(x[1]), False)
    sortedRDD.persist()
    
    #I use the accumulator to find percentage
    total_petitions = accum.value
    #The accumulator value is passed into the get_percentage and a new map is created with lang, page views and percentage
    mapFinalRDD = sortedRDD.map(lambda x: get_percentage(x, total_petitions))
  
    
    #print(accum.value)
    #Loop through to check all is correct
#     for f in mapFinalRDD:      
#       print(f)
      
    #Save to file
    mapFinalRDD.saveAsTextFile(o_file_dir)
# ------------------------------------------
# MAIN
# ------------------------------------------
if __name__ == '__main__':
    dataset_dir = "/FileStore/tables/A01_my_dataset/"
    o_file_dir = "/FileStore/tables/A01_my_result/"

    per_language_or_project = True
    dbutils.fs.rm(o_file_dir, True)
    my_main(dataset_dir, o_file_dir, per_language_or_project)
