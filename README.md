BDPA_Assign3_TWU
# MSc DSBA 

# Problems 0 - Pre-processing the input
In this assignment, you will use the document corpus of [pg100.txt](http://www.gutenberg.org/cache/epub/100/pg100.txt), as in your previous assignments, assuming that each line represents a distinct document (treat the line number as a document id). Implement a pre-processing job in which you will:


* Remove all StopWords (you can use the StopWords file of your previous assignment), special characters (keep only [a-z],[A-Z] and [0-9]) and keep each unique word only once per line. Don’t keep empty lines
* Store on HDFS the number of output records (i.e., total lines)} 
* Order the tokens of each line in ascending order of global frequency.


Note: Taking the whole complete file to run the similarity test took several hours to compile one run. In order to effectively present the result of this assignment, the document is reduced to the first 1000 lines as the input argument.

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

Here I tried a python-type mapping method but turns out not competiable to java-hadoop mapreduce. This first try is to use two for loop functions to all the possible combination. Later use another for loop function to delete the repeated duplicates. 

```
int nu = keylist.indexOf(key);
for(int i=0; i < nu+1; i++){decre.remove(i);}
			
StringBuilder stringBuilder = new StringBuilder();
for (String numi : decre)  {if (key.toString().equals(decre)) {continue;}
stringBuilder.append(key + "," + numi);
String k = stringBuilder.toString();
context.write(new Text(k.toString()), new Text(value.toString()));
```
This try result in a system crash that there is no output from reducer. Then I turn to find the solution on the internet. Some examples concerning Jarccord similarity use the custom set of Writable implementations that I also found it in the book  Hadoop - The Definitive Guide The Fourth Edition by Tom White. At page 121, he demonstrate the example code for an implementation that represents a pair of strings, called TextPair.

Basically I took the example and revise "compareTo" function becaue the example is made to compare two strings lexicographically. "If both the strings are equal then this method returns 0 else it returns positive or negative value. The result is positive if the first string is lexicographically greater than the second string else the result would be negative", quoted from the book. The example code can be used for the scenario:

```
[ 20 ] = [ 20 ]
 this    latter
```
However, our current task is to match out the duplicate like-----
```
[ 1233, 100 ] = [ 100, 1233 ] as duplicate
  this              late     <--- not specified enough to operate the comparison
```
To sum up, in this task, we have 4 integer values that we have to consider:
* this.first
* this.second
* late.first
* late.second


the duplicate will be defined as
```
this.first == this.second (skipped in the if condition already)

this.first == late.first  &&  this.second == late.second
      TF            LF             TS              LS
this.first == late.second &&  this.second == late.first
      TF            LS               TS             LF
```
Which is being realized by the following code:
```
int TFLFcmp = this.first.compareTo(late.first); 
int TSLScmp = this.second.compareTo(late.second); 
int TFLScmp = this.first.compareTo(late.second); 
int TSLFcmp = this.second.compareTo(late.first); 
if ( (TFLFcmp == 0 && TSLScmp == 0) || (TFLScmp == 0 && TSLFcmp == 0) ){return 0;}
```

Secondly, the following requirement is to design the condition to impose. in order to make the comparison, smaller values first; only when smaller values is the same, we turn to bigger value. The method I use is to always check if the front value minus back value is negative. Then it depends on negativity or positivity, we need to flip the position. According to these two constraints, we can tell which one is smaller and how to compare. 




```
				int thisflip = 0;
				int lateflip = 0;
				int opendoor = 0;
				int finalresult = 0;
				
				if (this.first.compareTo(this.second) < 0){
					thisflip = 1;
					}
				if (late.first.compareTo(late.second) < 0){
					lateflip = 1;
					}
				
				// policy: compare with smaller numbers -> if equal -> compare the other number
				// *(avoiding the explosive numbers)
```
The following code is to perform under the below logic:


number representation | Scenario | Action
------------ | ------------- | -------------
1 | t.first & l.first are smaller | both flip
2 | t.first is smaller + l.second is smaller | this flips
3 | t.second is smaller + l.first is smaller | later flips
4 | t.second is smaller + l.second is smaller | no flips in both


```
				if ( ( thisflip == 1 ) && (lateflip == 1) ){ 
					opendoor = 1;}
				if ( ( thisflip == 1 ) && (lateflip == 0) ){ 
					opendoor = 2;}
				if ( ( thisflip == 0 ) && (lateflip == 1) ){ 
					opendoor = 3;}
				if ( ( thisflip == 0 ) && (lateflip == 0) ){ 
					opendoor = 4;}
				
				if ( (opendoor == 1) ){ 
					if (TFLFcmp == 0) {
						finalresult = TSLScmp;
					} else {
						finalresult = TFLFcmp;
					}
				}
				
				if ( (opendoor == 2)){ 
					if (TFLScmp == 0) {
						finalresult = TSLFcmp;
					} else {
						finalresult = TFLScmp;
					}
				}
				
				if ( (opendoor == 3)){ 
					if (TSLFcmp == 0) {
						finalresult = TFLScmp;
					}else {
						finalresult = TSLFcmp;

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

In Reducer, I also utilize the function from TextPair, to get the first and second object in the pair. Then I can use the two value (in String form) to call the text values. By splitting them into HashSet, the Jaccard similarity can compare it by addAll, removeAll, and retainAll functions and count its size to generate similarity value. 

```
String[] ke = key.toString().split(",");
String keyone = ke[0];  
String keytwo = ke[1];    

HashSet<String> secondset = new HashSet<String>();
String twostrings = ProcessedDoc.get(keytwo);
for (String e : twostrings.split(" ")) {
	secondset.add(e);}
	
// firstset HashSet will be really similar to what I done to secondset HashSet	
```

The calculation and the output of Reducer, with threshold 0.8 as required.

```
double sim = JaSim(firstset, secondset);
if (sim >= 0.8) {context.write(new Text( "(" + key.toString()+ ")" ), new Text(String.valueOf(sim)) );}
```

[the output](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/A/part-r-00000) & [complete code](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/A/MDP022.java)

[](Comment text goes here)
![result](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/A/A.png)


# Problem 2 : Set-similarity joins (B)
Create an inverted index, only for the first $|d| - [t |d|] + 1$ words of each document d (remember that they are stored in ascending order of frequency). In your reducer, compute the similarity of the document pairs. Output only similar pairs on HDFS, in TextOutputFormat. Report the execution time and the number of performed
comparisons.

***

The mapper part is completely the same as Problem 1. I choose to manipulate the inverted index at Reducer, mainly. The method is the same to extract: using the ID from key, get the text in HashMap by key. Secondly, by splitting the whole string by space, having the numbers of the words as $d$, $t$ = threshold = 0.8

```
String keyone = key.getFirst().toString();  
String keytwo = key.getSecond().toString();    	

HashSet<String> secondset = new HashSet<String>();

String twostrings = ProcessedDoc.get(keytwo);
String[] Words = twostrings.split(" ");
long threshold_number = Math.round(Words.length - (Words.length * 0.8) + 1);
// https://www.tutorialspoint.com/java/number_round.htm
for (int r = 0; r < (int) threshold_number ; ++r ){
	secondset.add(Words[r]);}
	
```

Finally, [the output](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/B/part-r-00000) and [complete code](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/B/MDP022B.java)

![result](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/B/B.png)


# Problem 3 : Set-similarity joins (C)
Explain and justify the difference between a) and b) in the number of performed comparisons, as well as their difference in execution time.

***

comparison | number of performed comparisons | execution time
------------ | ------------- | -------------
a| 169071 | 1 mins 15 secs
b| 169071 | 1 mins 10 secs


Although having the same number of performed comparisons, I have less executation time in problem B. 

In the code, the problem A and problem B have the exactly same amount of pairs candidates that ran through counter. That was why I have the same number of performed comparisons. However, the biggest difference causing the time executation is that, in problem B, the cost in computing addAll, and retainAll. These two functions take much less words to perform the jobs in problem B than in problem A, because the size of the HashSet has been reduced shorter by the equation. 

As a result, problem B has much less time wasted on Jaccard Similiarity function than the time in problem A, benefit from below equation. 
```
|d| - [t |d|] + 1
```

The console log can be viewed [here](https://github.com/thwowu/BDPA_Assign3_TWU/blob/master/log)
