# BDPA_Assign3_TWU



```


```




# Problems 0 

* Remove all StopWords (you can use the StopWords file of your previous assignment), special characters (keep only [a-z],[A-Z] and [0-9]) and keep each unique word only once per line. Don’t keep empty lines
* Store on HDFS the number of output records (i.e., total lines)} 
* Order the tokens of each line in ascending order of global frequency.



# Problem 1 : Set-similarity joins (A)
Perform all pair-wise comparisons between documents, using the following technique: Each document is handled by a single mapper (remember that lines are used to represent documents in this assignment). The map method should emit, for each document, the document id along with one other document id as a key (one such pair for each other document in the corpus) and the document’s content as a value. In the reduce phase, perform the Jaccard computations for all/some selected pairs. Output only similar pairs on HDFS, in TextOutputFormat. Make sure that the same pair of documents is compared no more than once. Report the execution time and the number of performed comparisons.


### Assignment Requirement Analysis
* Each document is handled by a single mapper
* For each document, the document id along with one other document id as a key (one such pair for each other document in the corpus) and the document’s content as a value
* Same pair of documents is compared no more than once.
* Jaccard computations in Reducer
* Output only similar pairs on HDFS, in TextOutputFormat.
* Report the execution time and the number of performed comparisons

*** 

According to the assignment instruction, the first point is to compare a pair of documents, which can be string. The second point is to have a method to create index number for each line, which can be regarded as a document in this case. As a result, in this exercise, firstly it is required to have a file that should have a index value to be the document ID, and its context after the document ID. Each document ID is served to present a line and do the configuration to read key and values, separated by commas (because the file is stored in .csv format ) from the input files

It allows us to pair ID with each other, in response to “The map method should emit, for each document, the document id along with one other document id as a key (one such pair for each other document in the corpus) and the document’s content as a value. ” As a result, at the intermediate output from Mapper, key verse value should be something similar to a pair of numbers (Document ID) verse each line’s context (without stopwords). 
 

According to the instruction, “In the reduce phase, perform the Jaccard computations for all/some selected pairs.”, in order to perform Jaccard computations, it is necessary to create a new function to make the calculation in any pair candidates. \\

What we have here, are 1) the candidates to be compare, with its document ID (line) 2) only the corpus from the 1st element in the par. By extracting the key coming from mapper, we can obtain the 2nd element’s document ID. Then we use the ID number to call back its corresponding corpus, with the help of importing the same file which was imported in mapper as well.\\

After that, I can circulate the stream of each corpus into Jaccard computation to get a value for each pair. Register one iteration as the system performed one time of comparison. The required threshold similarity > 0.8, is set as the condition in the output. Stepping further to look at Question II, where demands to “Output only similar pairs on HDFS, in TextOutputFormat. Report the execution time and the number of performed comparisons.”, it is necessary to write the pair, accompany with its similarity and text.

Referencing from the tutorial of the book Hadoop – The Definitive Guide, written by Tom White, at page 149, he introduces an implementation that represents a pair of strings, called TextPair with its example code.


The original objective the bone code, is designed to count the frequency of the occurrence of two words together in the text. I plan to use the idea of holding two words, and transform the idea into holding two values (in our case, it is the index of lines that the documents belonging to).

The further transformation from this bone code, is to equip with the capability of removing the duplicated set of pairs, since the requirement mentioning that “Make sure that the same pair of documents is compared no more than once” to save the time spent on computation. 


So we are going to define a custom class that is going to hold the two words together.

# (This may be the most platform independent comment)
![result](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/A/A.png)


# Problem 2 : Set-similarity joins (B)
Create an inverted index, only for the first $|d| - [t |d|] + 1$ words of each document d (remember that they are stored in ascending order of frequency). In your reducer, compute the similarity of the document pairs. Output only similar pairs on HDFS, in TextOutputFormat. Report the execution time and the number of performed
comparisons.
    
![result](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/B/B.png)


# Problem 3 : Set-similarity joins (C)
Explain and justify the difference between a) and b) in the number of performed comparisons, as well as their difference in execution time.

