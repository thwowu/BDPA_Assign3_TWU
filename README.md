BDPA_Assign3_TWU
# a student of MSc DSBA 

***

# Problems 0 - Pre-processing the input
In this assignment, you will use the document corpus of [pg100.txt](http://www.gutenberg.org/cache/epub/100/pg100.txt), as in your previous assignments, assuming that each line represents a distinct document (treat the line number as a document id). Implement a pre-processing job in which you will:


* Remove all StopWords (you can use the StopWords file of your previous assignment), special characters (keep only [a-z],[A-Z] and [0-9]) and keep each unique word only once per line. Don’t keep empty lines
* Store on HDFS the number of output records (i.e., total lines)} 
* Order the tokens of each line in ascending order of global frequency.


Note: 

At the first time of running this, I took the whole complete file to run the similarity test. The pre-prossessing part was okay, but as soon as I run the first comparison job, the system crashed (I ran the code overnight more than 7 hours and checked the result in the morning. The console stopped at 5% mapper). Invitably, I will have to reduce the numbers of lines considering my hardware condition is not competiable to run a big mapreduce job. Then 

Secondly, I reduce the numbers of line to 10,000 lines, but it took several hours to compile just one run. It was still not ideal beceause I would not be able to do anything or modify if the job will take me several hours to run one job, and my computer got completely frozen during the run. Finally, I found the balance of not taking too less lines, but still keep the running time in reasonable time. I would have the first 1200 lines to be the input argument of this project.

As a result, the following result is presented by this pre-condition after catastrophic crashes during several nights. 

***

### StopWords Utilization

Learned from the last assignment, we have had the StopWords. For this assignment pre-prossessing part, I import the StopWords list ([StopWords Input File](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/Pre-Processing/read_input/stopWords.csv), extracting from pg100.txt) to eliminate the high frequency apparence words (>4000 times occurrence) and use pattern to filter out. 

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
		    	 
	if (token.toLowerCase().isEmpty() || stopWords.contains(token.toLowerCase() ))  {
	  continue;} 		 
	if (!m.find() || value.toString().length() == 0  ) {
	  continue;} 
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
	counter};
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
Therefore we can revise the code and turn them into the following code for storing the WordCount information, and this is [the input file](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/Pre-Processing/read_input/WordCount.csv) that I use. Instead of taking all three documents together in the previous assignment, this global frequency index is * only * generated from pg100.txt, in order to match the requirement of this assignment. 


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

Then I have to put back the chunks back together by Stringbulder. Consdering that there's no further need to splitting, for later use in question A and question B, here I try to eliminate the hashtag and let the different words seperated only by space. At the end of the code, I will use the global counter to print out the sequential number to be the key of Reducer output. 

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
	         String nujj = String.valueOf(context.getCounter(UNIQUE.counter).getValue());
	         
	         
	         context.write(new Text(nujj.toString()), new Text(newsortedbyFreq.toString()
		     		.replaceAll("#\\d+", "").replaceAll(",", "").replaceAll("__#null", "")));
```
Therefore, the output of the reducer finally comes out like this
```
1,golden silver red blue kitkat
2,context silver red finally kitkat
3,golden context red blue kitkat
4,golden silver context finally kitkat

```

[Complete code](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/Pre-Processing/MDP02Pre.java) and [Output file](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/Pre-Processing/part-r-00000) as the input for later Question A and Question B.


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

#### Each document is handled by a single mapper

As the requirement demands, the previous preparasion in the [pre-processing output](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/A/rawinput.csv), can be used as input which has the unique value for each line as the document ID, and its filtered corpus. Thus, the first thing to add to code is the configuration to indicate the input format for mapper. Because I stored the last output as csv by a commas, therefore I define my separator here as commas so the mapper can quickly take the key & value correctly. 

```
job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
```

#### Key & Value in Mapper

Bascially, the first half is very similar to the pre-processing part, but the import file is very different this time. This time i import the same file as input file, where comes from pre-processing output. The purpose is that I want to pull out the doucment IDs from other sources, so that, when the key is pointing at one documents ID, I will have a list of all the possible candidates to map out to the pair. 

```
HashSet<String> Di = new HashSet<String>();
rdr = new BufferedReader(new FileReader(new File("/home/cloudera/workspace/MDP02Pre/rawinput.csv")));
String pattern;
while ((pattern = rdr.readLine()) != null) {
	String[] word = pattern.split(",");
	Di.add(word[0]);
```

First of all, I start by creating a for loop to group all Document ID with any possible combination. Upon finished, there are 2 conditions to make sure to achieve original objective, 1. if ID numbers are the same, we don't register; 2. transforming the string into integer to avoid writing the latter element in a pair is lower than the first element. For example, the pair (3,1) has been created when creates (1,3) in round (1) = Keystring. 

If the key and corresponding document ID are satisfying with prosposed conditions, the system will output the pair as a Text to pass to Reducer. 

```
String ID = key.toString();
for (String line : Di) {
	Pattern p = Pattern.compile("[0-9]");
	String Keystring = key.toString();
	Matcher m = p.matcher(Keystring.trim());
		 if (Keystring.isEmpty() ) {continue;} 
		 if (!m.find()) {continue;}
		 if ( ID.equals(line) ) {continue;}
	StringBuilder stringBuilder = new StringBuilder();
				
	int wo = Integer.parseInt(Keystring);
	int lineline = Integer.parseInt(line);
	if ( wo > lineline ) {continue;}
	stringBuilder.append(Keystring + "," +line);
	context.write(new Text(stringBuilder.toString()), new Text(value.toString() ) );
	}
```

So far the intermediate output from mapper will look like as 
```
key: 230
value: define wind custom breathe deep going learning
```

***

After finishing Mapper alteration, in Recducer, I implement a new function to calculate Jaccard similarity, inpsired by [jaccard similarity index-for measuring document similarity] (https://nickgrattan.wordpress.com/2014/02/18/jaccard-similarity-index-for-measuring-document-similarity/) and the collection function learned from [this discussion page](http://stackoverflow.com/questions/13648391/collection-addall-removeall-returns-boolean) for addAll, removeAll, and retainAll . 
```
public double JaSim(HashSet<String> hs1, HashSet<String> hs2){
	Set <String> Intersection = new HashSet <String> (hs1);
	Set <String> Union = new HashSet <String> (hs2);
			
	Intersection.retainAll(hs2); // hs1 intersects with hs2 
	Union.addAll(hs1); // hs2 +++ hs1 is the union *(putting all together without duplicate)
	int InS = Intersection.size();
	int Uni = Union.size();
	
	return (double) InS / Uni ;
	} 
```

In Reducer, I extract the information of the first and second object from the key and split them into two parts and store individually. Then I can use the two value (in String form) to call the text values. By splitting them into HashSet, the Jaccard similarity can compare it by addAll, removeAll, and retainAll functions and count its size to generate similarity value. 

```
String[] ke = key.toString().split(",");
String keyone = ke[0];  
String keytwo = ke[1];    

HashSet<String> firstset = new HashSet<String>();
String onestrings = ProcessedDoc.get(keyone);
for (String f : onestrings.split(" ")) {
	firstset.add(f);}

HashSet<String> secondset = new HashSet<String>();
String twostrings = ProcessedDoc.get(keytwo);
for (String e : twostrings.split(" ")) {
	secondset.add(e);}
```

The calculation and the output of Reducer, with threshold 0.8 as required.

```
double sim = JaSim(firstset, secondset);
if (sim >= 0.8) {context.write(new Text( "(" + key.toString()+ ")" ), new Text(String.valueOf(sim)) );}
```

[the output](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/A/part-r-00000) & [complete code](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/A/MDP022.java)

![result](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/A/A.png)


# Problem 2 : Set-similarity joins (B)
Create an inverted index, only for the first $|d| - [t |d|] + 1$ words of each document d (remember that they are stored in ascending order of frequency). In your reducer, compute the similarity of the document pairs. Output only similar pairs on HDFS, in TextOutputFormat. Report the execution time and the number of performed
comparisons.

***

The mapper part is similar as Problem A, but the major difference coming from the more computation saving method that i put in the intermediate output of my mapper. 

As you can see from the below code, I put the IDs and corpus together, following by this form: Document One ID , Document Two ID, Document One Corpus, Document Two corpus. Then in my reducer, I can simply split them by commas, and assign them into individual string for later comparison. 

In this way, I save the time that wasted on reading in rawinput.cvs in every reducer, where I have the number of reducer euqal to the number of pair candidates. If I have 1000 candidate, this new method just saves me the 1000 times of read-in memory from importing rawinput.cvs 1000 times. 


```
stringBuilder.append(Keystring + "," +line + "," + value.toString() + "," + Di.get(line));
context.write(new Text(stringBuilder.toString()), new Text(value.toString() ) );
```

So far the intermediate output from mapper will look like as 
```
key: 230,901,define wind custom breathe deep going learning,The method is the same to extract
   (1st ID ) / (2nd ID) / (1st corpus) / (2nd corpus)
value: define wind custom breathe deep going learning
   (we dont use this in reduce in fact)

*** Note: I try to integret two corpus to be stored in values. However, from many experiments I did, for exmaple, the value of mapper output as (1st corpus) / (2nd corpus), but it always crashes in reducer. My guess is because in reducer, the value of input argurment is iterable value. Somehow it just does not accept that I will split the value into strings. So far i could not find the specific reason to explain this fact yet. 

```


I choose to manipulate the inverted index at Reducer, mainly. The method is the same to extract: using the ID from key, get the text in HashMap by key. Secondly, by splitting the whole string by space, having the numbers of the words as $d$, $t$ = threshold = 0.8. And them by using for loop function, to put back the words into HashSet one by one

```
String[] ke = key.toString().split(",");
String keyone = ke[0];  
String keytwo = ke[1];  	
String onestrings = ke[2];
String twostrings = ke[3];    	

HashSet<String> secondset = new HashSet<String>();
String[] Words = twostrings.split(" ");
long threshold_number = Math.round(Words.length - (Words.length * 0.8) + 1);
for (int r = 0; r < (int) threshold_number ; ++r ){
	secondset.add(Words[r]);}

HashSet<String> firstset = new HashSet<String>();
String[] Words_one = onestrings.split(" ");
long threshold_numberone = Math.round(Words_one.length - (Words_one.length * 0.8) + 1);
for (int h = 0; h < (int) threshold_numberone ; ++h ){
					 firstset.add(Words_one[h]); }
```

In between putting back the strings into Hashset and executing the similarity comparison, here I also implement a "if" function to distinguish the obvious answer. The simple logic is that if there is only one left in the HashSet, we can easily use contain function to test and return a boolean response, without involving with the comparison function. 

There are three steps to verify the similiary,

First of all, i check if there is at least one match, if so, the integer variable will increase one by a for loop (it breaks when the intgert is larger than zero). If the for loop continues until the end, the integer will be alwasy zero without break, so that means there is NO match in both HashSet. We can simply state that the similarity is zero, or to say, there is no similarity in this pair of candidates.

Secondly, i check the candidate pairs which either of them only has one word after the elimination. Because the only one word can quickly decide whether there is a match or not (simply think as there is no other possibilities but the unique one word in either HashSet). Therefore, before we actually compute the Jaccard similarity, we know already the similary is either one or zero.

Thirdly, if my candidate pair has at least one match, and both of them have more than one word in each set, we start to perform Jaccard Similarity comparison (where the counter actually clicks) and pass through the 0.8 threshold filter to write out the output in HDFS. 


At the same time, the output of reduce is edited to fit into the new method too. 

```
double sim = 0;
int bobo = 0;
			for (String hw : Words_one){
				if (secondset.contains(hw)){ 
				bobo = bobo + 1;}
				if (bobo > 0){
				break;}
			}
			
			if (bobo < 1){
				sim = 0;}
			
			else{
				if ((int) threshold_numberone == 1){
					if (secondset.contains(Words_one[0])) {
						sim = 1;}
					else{
						sim = 0;}
				}
				else if ((int) threshold_number == 1) {
					if (firstset.contains(Words[0])) {
						sim = 1;}
					else{
						sim = 0; }
				}
				else{
				context.getCounter(UNIQUE.counter).increment(1);
				sim = JaSim(firstset, secondset); }	
			}	
			if (sim >= 0.8) {
			context.write(new Text( "(" + keyone + "," + keytwo + ")" ), new Text(String.valueOf(sim)) );
			}
```

As a reuslt, we can obtain the output to be in this form too:

```
(121,298),1.0
(126,133),1.0
(161,1003),1.0
```

Finally, [the output](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/B/part-r-00000) and [complete code](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/B/MDP022B.java)

![result](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/B/B.png)


# Problem 3 : Set-similarity joins (C)
Explain and justify the difference between a) and b) in the number of performed comparisons, as well as their difference in execution time.

***

comparison | number of performed comparisons | execution time
------------ | ------------- | -------------
a| 530,878 | 10 mins 29 secs
b| 1,664 | 1 mins 41 secs


Although having the same number of performed comparisons, I have less executation time in problem B. 

In the code, the problem A and problem B have the very different amount of pairs candidates that ran through counter. The biggest difference causing the time executation are two reasons. First of all, comparing to the A, without the contrainst of appointing the content of mapping output, in B, I do not need to read in rawinput.csv in each reducer for mapping out the corpus. Instead, I can directly take the corpus from my reducer's key, so that my reducer only focus on checking comparison after certain if functions I define at the beggining. Seconly, in problem B, the cost in computing addAll, and retainAll. These two functions take much less words to perform the jobs in problem B than in problem A, because the size of the HashSet has been reduced shorter by the equation. 

As a result, problem B has much less time wasted on Jaccard Similiarity function than the time in problem A, benefit from below equation and the simplied corpus mapping method implemented in B. 
```
|d| - [t |d|] + 1
```

The console log can be viewed [here](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/log)
