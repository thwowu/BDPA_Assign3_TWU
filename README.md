# BDPA_Assign3_TWU







# Problems 0 

* Remove all StopWords (you can use the StopWords file of your previous assignment), special characters (keep only [a-z],[A-Z] and [0-9]) and keep each unique word only once per line. Don’t keep empty lines
* Store on HDFS the number of output records (i.e., total lines)} 
* Order the tokens of each line in ascending order of global frequency.


***

### StopWords Utilization

Learned from the last assignment, we have had the StopWords. For this assignment pre-prossessing part, I import the StopWords list ([WordCount Input File](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/Pre-Processing/read_input/stopWords.csv), extracting from pg100.txt) to eliminate the high frequency apparence words (>4000 times occurrence) and use pattern to filter out. 

```
HashSet<String> stopWords = new HashSet<String>();
	    	 rdr = new BufferedReader(new FileReader(new File("/home/cloudera/workspace/MDP01D/stopWords.csv")));
	    	 String pattern;
				while ((pattern = rdr.readLine()) != null) {
					String[] word = pattern.split(",");
					stopWords.put(word[0]);} 
	    	// http://stackoverflow.com/questions/1625814/get-a-hashset-out-of-the-keys-of-a-hashmap

	    	 for (String token: value.toString().split("\\s*\\b\\s*")) {
	        	 token = token.trim().toLowerCase();
	        	 Pattern p = Pattern.compile("^[a-zA-Z0-9]");
		    	 Matcher m = p.matcher(token.toLowerCase());
		    	 
	        	 if (token.toLowerCase().isEmpty() 
	        	     || stopWords.contains(token.toLowerCase() ))  {
	    	             continue;
	    	             } 		 
	        	 if (!m.find() || value.toString().length() == 0  ) {
	                 continue;
	             	     } 
```

After previous steps, I write remaining non-stopwords words as output value, and key becomes the numbers of counted characters starting from the first characters, which will be served as the document ID keys for this assignment, since it fits the requirement of uniqueness (it will keep increasing, instead of meeting the duplicate situation). 

```
context.write(key, new Text(token.toLowerCase()));
```

### Counter & its Output

From the assignment I, I already implemented a code that printing out the counter value at the CONSOLE, where I took the reference from [imageterrier](http://www.imageterrier.org/HadoopImageTerrier/apidocs/org/terrier/indexing/HadoopIndexerMapper.Counters.html). 
```
Counters counters = job.getCounters();
Counter c1 = counters.findCounter(UNIQUE.counter);
System.out.println("Numbers of output number is: " + c1.getValue());
```
```
public static enum UNIQUE {
		 counter
		 };
```

According to [how to read and write text file in java](http://www.codejava.net/java-se/file-io/how-to-read-and-write-text-file-in-java), I learned that I can further organize the results by creating a output txt file, saving from exploring in the compile log. 

Firstly this is the example file that I was referencing from:
```
	    FileWriter writer = new FileWriter("MyFile.txt", true);
            BufferedWriter bufferedWriter = new BufferedWriter(writer);
 
            bufferedWriter.write("Hello World");
            bufferedWriter.newLine();
            bufferedWriter.write("See You Again!");
```

Thus, I edited the code and re-organize into the following code to fit my requirements:
```	     
	      FileWriter writer = new FileWriter("MyFile.txt", true);
              BufferedWriter bufferedWriter = new BufferedWriter(writer);
              bufferedWriter.write( String.valueOf( c1.getValue()) );	
              bufferedWriter.close();
```
where the counter is embedded right before the reducer output 
```
context.getCounter(UNIQUE.counter).increment(1);
```

### Ascending Order of Global Frequency

The goal in the reducer can be divided into two three parts: Firstly, we can use the global frequency, more specifically speaking, WordCount in assignment 0. Secondly, we use the technique used in the assignment 2 to insert the frequency to the specific word. Lastly, the most challenging part is to sort a list by its elements' integers. 

Firstly, we import the global frequency by the followings. Apart from using the HashSet, here referecing from [WordCount result to be the number in the later use](http://stackoverflow.com/questions/16246821/how-to-get-values-keys-from-hashmap), considering that I would like to call the values (frequency) by its word (key),  this example fits the requirement perfectly.
```
Set set = (Set) map.entrySet();
Iterator it = set.iterator();
while(it.hasNext()){
    Map.Entry entry = mapIterator.next();
    System.out.print(entry.getKey() + " : " + entry.getValue());
}
```
Therefore we can revise the code and turn them into the following code for storing the WordCount information, and this is [the input file](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/Pre-Processing/read_input/WordCount.csv) that I use :
```
HashMap<String, String> wordcount = new HashMap<String, String>();
reader = new BufferedReader(new FileReader(new File("/home/cloudera/workspace/WordCount/output/WordCount.csv")));

String pattern;
	while ((pattern = reader.readLine()) != null) {
		String[] word = pattern.split(",");
		wordcount.put(word[0], word[1]);}
```

Then perform routing the global frequency into the similar format as last assignment (document_file_name#frequency -> word#frequency). Keeping the # can help the later work when the hashtag # works as a seperator that I can split them into two groups. 

```
for (Text num : values) {title.add(num.toString());}

HashSet<String> ti = new HashSet<String>(title);  
StringBuilder stringBuilder = new StringBuilder();
     	 
for (String numi : ti) { 
	if (numi.length() > 0);{
	if (stringBuilder.length() > 0) stringBuilder.append(", ");{	        		
	    stringBuilder.append(numi + "#" + wordcount.get(numi));}
	    }
}
```
So far at this step, I have the stream of value as 
```
golden#3, silver#10, red#30, blue#200, kitkat#2
```



Finally, after considering several methods to solve the problem including sort(), or Arrays.sort(), the youtube video introduces how to sort Lists - ArrayLists and LinkedLists - in terms of Comparators, and how to make use of the Collection interface which is very useful in this assignment since the sorted is done per line. 
[![IMAGE ALT TEXT HERE](https://i.imgur.com/zR2KWD7l.png)](https://www.youtube.com/watch?v=QYvUmIYgsiA)

```
List<Integer> numbers = new ArrayList<Integer>();
numbers.add(3);
numbers.add(36);
numbers.add(1);
Collections.sort(numbers, new Comparator<Integer>() {
public int compare(Integer num1, Integer num2) {
	return num1.compareTo(num2);
```
Here I need to firstly split them by commas so they will be broken into chunks as List elements to iterate. Then I re-use the pattern to group two pattern seperately: "non-integer characters and #" & "integer", because I would like to compare the sequence and sort them according to its value. 

Then I have to put back the chunks back together by Stringbulder. Consdering that there's no further need to splitting, for later use in question A and question B, here I try to eliminate the hashtag and let the different words seperated only by space. 
```
golden silver red blue kitkat
```
and, the code works like that:

```
List<String> rangingData = Arrays.asList(stringBuilder.toString().split("\\s*,\\s*")); 
Collections.sort(rangingData, new Comparator<String>() {
	public int compare(String o1, String o2) {
		return removeInt(o1) - removeInt(o2);}

	int removeInt(String s) {
		String work = s.trim();
		Pattern p = Pattern.compile("(\\w+#)(\\d+)");
		Matcher m = p.matcher(work);
		String wor = new String();
		while (m.find()){
			wor = m.group(2);}
		if (wor.isEmpty()){
			return 0;}
	    	int wo = Integer.parseInt(wor);
		// http://stackoverflow.com/questions/5585779/converting-string-to-int-in-java
		return wo;} } );
		
StringBuilder newsortedbyFreq = new StringBuilder();
	         
	for (String numi : rangingData) { 
		if (numi.length() > 0);{
		if (newsortedbyFreq.length() > 0) newsortedbyFreq.append(" ");{	        		
		   newsortedbyFreq.append(numi.replaceAll("#\\d+", ""));
		         } } }
	         
	         /* Book - MapReduce Design Patterns p.160
	          * If it is empty or null, increment the NULL_OR_EMPTY_COUNTER counter by 1
	          * context.getCounter(STATE_COUNTER_GROUP, NULL_OR_EMPTY_COUNTER).increment(1);
	          */
	context.getCounter(UNIQUE.counter).increment(1);
	         
	context.write(key, new Text(newsortedbyFreq.toString()
	       .replaceAll("#\\d+", "").replaceAll(",", "").replaceAll("__#null", "")));
```
Therefore, the output of the reducer finally comes out like this
```
1000,golden silver red blue kitkat
```

[Complete code](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/Pre-Processing/MDP02Pre.java) and [Output file](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/Pre-Processing/part-r-00000) for later Question A and Question B



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

[](Comment text goes here)
![result](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/A/A.png)


# Problem 2 : Set-similarity joins (B)
Create an inverted index, only for the first $|d| - [t |d|] + 1$ words of each document d (remember that they are stored in ascending order of frequency). In your reducer, compute the similarity of the document pairs. Output only similar pairs on HDFS, in TextOutputFormat. Report the execution time and the number of performed
comparisons.
    
![result](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/B/B.png)


# Problem 3 : Set-similarity joins (C)
Explain and justify the difference between a) and b) in the number of performed comparisons, as well as their difference in execution time.

