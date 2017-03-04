package mdp022;

import java.io.IOException;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput; // for TextPair class
import java.io.DataOutput; // for TextPair class
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable; // for TextPair class
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat; // org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

class TextPair implements WritableComparable<TextPair> {
			private Text first; 
			private Text second;
			
			/*
			 * The first part of the implementation is straightforward: 
			 * there are two Text instance variables, first and second, 
			 * and associated constructors, getters, and setters.
			 */
			
			public TextPair() {
				set(new Text(), new Text());
				 }
			public TextPair(String first, String second) {
				set(new Text(first), new Text(second));
			}
			
			public TextPair(Text first, Text second) {
				set(first, second);
			}
				 
			public void set(Text first, Text second) {
				this.first = first;
				this.second = second; 
			}
			
			public Text getFirst() { 
				return first;
			}
			
			public Text getSecond() { 
				return second;
			}
			
			/*
			 * All Writable implementations must have a default constructor so that the MapReduce framework 
			 * can instantiate them, then populate their fields by calling readFields(). 
			 * 
			 * Writable instances are mutable and often reused, so you should take care to 
			 * avoid allocating objects in the write() or readFields() methods.
			 * 
			 */
			
			
			@Override
			public void write(DataOutput out) throws IOException { 
				first.write(out);
				second.write(out);
			}
			
			/*
			 * TextPair’s write() method serializes each Text object in turn to the output stream 
			 * by delegating to the Text objects themselves.
			 */
			
			
			@Override
			public void readFields(DataInput in) throws IOException { 
				first.readFields(in);
				second.readFields(in);
			}
			
			/* p.123 from book - Hadoop-The.Definitive.Guide_4.edition_a_Tom.White_April-201
			 * 
			 * readFields() de-serializes the bytes from the input stream by delegating to each Text object.
			 * 
			 * The DataOutput and DataInput interfaces have a rich set of methods 
			 * for serializing and de-serializing Java primitives, 
			 * so, in general, you have complete control over the wire format of your Writable object.
			 */
			
			@Override
				public int hashCode() {
				return first.hashCode() * 163 + second.hashCode();
				}
			
			/*
			 * The hash Code() method is used by the HashPartitioner 
			 * (the default partitioner in MapReduce) to choose a reduce partition, 
			 * 
			 * so you should make sure that you write a good hash function that 
			 * mixes well to "ensure reduce partitions are of a similar size".
			 */		
			
			@Override
			public boolean equals(Object o) { 
				if (o instanceof TextPair) {
				      TextPair late = (TextPair) o;
				return first.equals(late.first) && second.equals(late.second); 
				}
				
				return false; 
			}
			
			@Override
				public String toString() { 
				return first + "," + second;
			}
			/*
			 * If you plan to use your custom Writable with TextOutputFormat, 
			 * you must implement its toString() method. 
			 * 
			 * TextOutputFormat calls toString() on keys and values for their output representation. 
			 * For TextPair, we write the underlying Text objects as strings separated by a tab character.
			 * 
			 */
					
			@Override 
				public int compareTo(TextPair late){
				
				/*
				 * 
				 * The method compareTo() is used for comparing two strings lexicographically. 
				 * Each character of both the strings is converted into a Unicode value for comparison. 
				 * 
				 * If both the strings are equal then this method returns 0 else it returns positive or negative value. 
				 * The result is positive if the first string is lexicographically greater 
				 * than the second string else the result would be negative.
				 * 
				 * 
				 * 
				 * 
				int cmp = first.compareTo(late.first); 
				
				if(cmp!=0){       // when the prior (first) and latter (second) is not equal
					return cmp;   // return an integer number
				}

				 * Note: 
				 * The example in the book is not suitable to be applied to the case that we are doing
				 * 
				 * the book example is useful when it comes to------
				 * [ 20 ] = [ 20 ]
				 *  this      late
				 * 
				 * Current Task want to match out the duplicate like-----
				 * [ 1233, 100 ] = [ 100, 1233 ] as duplicate
				 *     this              late
				 * 
				 * This is definitely not going to work out for sure.
				 * 
				 * To sum up, in this task, we have 4 integer values that we have to consider:
				 * this.first
				 * this.second
				 * late.first
				 * late.second
				 * 
				 * the duplicate will be defined as
				 * 
				 * 0) this.first == this.second (skipped in the if condition already)
				 * 
				 * 1) this.first == late.first  &&  this.second == late.second
				 *       TF            LF             TS              LS
				 * 
				 *
				 * 2) this.first == late.second &&  this.second == late.first
				 *       TF            LS               TS             LF
				 * 
				 * can be fulfilled in the first if condition.
				 * 
				 * 
				 * The following requirement is to design the condition to 
				 * impose the ordering you would expect: 
				 * it sorts by the first string followed by the second
				 * 
				 * a. the first numbers are different;
				 * b. the first number are the same, but second numbers are different
				 * 
				 */
				
				int TFLFcmp = this.first.compareTo(late.first); 
				int TSLScmp = this.second.compareTo(late.second); 
				int TFLScmp = this.first.compareTo(late.second); 
				int TSLFcmp = this.second.compareTo(late.first); 
				
				if ( (TFLFcmp == 0 && TSLScmp == 0) || (TFLScmp == 0 && TSLFcmp == 0) ){
				return 0;
				}
				
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
				
				// t.first & l.first are smaller ---> 1 *(both flip) 
				// t.first is smaller + l.second is smaller ---> 2 *(this flips)
				// t.second is smaller + l.first is smaller ----> 3 *(later flips)
				// t.second is smaller + l.second is smaller ----> 4 *(no flips in both)
				
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
					}
				}
				
				if ( (opendoor == 4)){ 
					if (TSLScmp == 0) {
						finalresult = TFLFcmp;
					}else {
						finalresult = TSLScmp;
					} }
				
				return finalresult;
			}
		}


public class MDP022 extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new MDP022(), args);	
		System.exit(res);
		   }

	@Override
	public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "MDP022");
	      job.setJarByClass(MDP022.class);
	      job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class); 

	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);
	      job.setNumReduceTasks(1);
     
	      job.setMapOutputKeyClass(TextPair.class);  // ref. p.273, Hadoop Definition 4th
	      job.setMapOutputValueClass(Text.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);
	      job.setInputFormatClass(KeyValueTextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);
	      	      
	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));
	      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
	      job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
	      
	      FileSystem fs = FileSystem.newInstance(getConf());

	      if (fs.exists(new Path(args[1]))) {
				fs.delete(new Path(args[1]), true);}
	      
	      
	      job.waitForCompletion(true);			


      	  // Print the COUNTERS Values on the CONSOLE
	      // http://www.imageterrier.org/HadoopImageTerrier/apidocs/org/terrier/indexing/HadoopIndexerMapper.Counters.html
	      Counters counters = job.getCounters();
	      Counter c1 = counters.findCounter(UNIQUE.counter);
	      
	      System.out.println("Numbers of output number is: " + c1.getValue());
	      
	      // http://www.codejava.net/java-se/file-io/how-to-read-and-write-text-file-in-java 
	      /*
	        FileWriter writer = new FileWriter("MyFile.txt", true);
            BufferedWriter bufferedWriter = new BufferedWriter(writer);
 
            bufferedWriter.write("Hello World");
            bufferedWriter.newLine();
            bufferedWriter.write("See You Again!");
	       */
	      FileWriter writer = new FileWriter("MyFile.txt", true);
          BufferedWriter bufferedWriter = new BufferedWriter(writer);
          bufferedWriter.write( String.valueOf( c1.getValue()) );	
          bufferedWriter.close();
          
	      return 0;
	}
	
	public static enum UNIQUE {
		 counter
		 };
		 
	public static class Map extends Mapper<Text, Text, TextPair, Text> {
		  private BufferedReader rdr;
		  private static TextPair Pair = new TextPair();
		  
	      @Override
	      public void map(Text key, Text value, Context context)
	              throws IOException, InterruptedException {
	    	 
	    	//HashMap<String, String> Dict = new HashMap<String, String>();
	    	HashSet<String> Di = new HashSet<String>();
	    	rdr = new BufferedReader(new FileReader(
								new File("/home/cloudera/workspace/MDP02Pre/rawinput.csv")));
	    	
	    	//ArrayList<String> keylist = new ArrayList<String>();
	    	//ArrayList<String> decre = new ArrayList<String>();
	    	
	    	String pattern;
				while ((pattern = rdr.readLine()) != null) {
					String[] word = pattern.split(",");
					//Dict.put(word[0], word[1]);
					Di.add(word[0]);
					// keylist.add(word[0]);
					// decre.add(word[0]);
				} 
			
			/*
			 * Originally Mapper *(Failure Corpse)
			 * 
			
			int nu = keylist.indexOf(key);
			for(int i=0; i < nu+1; i++){
				decre.remove(i);
			}
			
			StringBuilder stringBuilder = new StringBuilder();
			
			for (String numi : decre)  {
				if (key.toString().equals(decre)) {
					continue;
				}

				stringBuilder.append(key + "," + numi);
				String k = stringBuilder.toString();
				
				context.write(new Text(k.toString()), new Text(value.toString()));
			}
			*/
			
			String ID = key.toString();
			// to take key as Document ID and group it with any possible combination	
			for (String line : Di) {
				// upon finished, there are 2 conditions to make sure to achieve original objective
				// 1. if ID numbers are the same, we don't register
				// 2. using TextPair class to check if there are duplicate ex: (0,630) = (630,0)
				Pattern p = Pattern.compile("[0-9]");
				
				String Keystring = key.toString();
		    	Matcher m = p.matcher(Keystring.trim());
		    	if (Keystring.isEmpty() ) {
	                continue;} 
		    	if (!m.find()) {
	           		continue;}
				if ( ID.equals(line) ) {
					continue;}
				
				Pair.set(new Text(Keystring), new Text(line) );
				
				context.write(Pair, new Text(value.toString() ));
			}
	     }
	}
	   

	public static class Reduce extends Reducer<TextPair, Text, Text, Text> {
		 private BufferedReader reader;
		 
		 /* inspired by this blog, originally from C# code 
		  * https://nickgrattan.wordpress.com/2014/02/18/jaccard-similarity-index-for-measuring-document-similarity/
		  * 
		  * public double Cal(HashSet<String> hs1, HashSet<String> hs2){
		  *     return ((double)hs1.Intersect(hs2).Count() / (double)hs1.Union(hs2).Count());}
		  *     
		  */
		 
		 public double JaSim(HashSet<String> hs1, HashSet<String> hs2){
			 
			 HashSet <String> Intersection = new HashSet <String> (hs1);
			 HashSet <String> Union = new HashSet <String> (hs2);
			
			 Intersection.retainAll(hs2); // hs1 intersects with hs2 
			 Union.addAll(hs1); // hs2 +++ hs1 is the union *(putting all together without duplicate)
		     
		     int Uni = Union.size();
		     int InS = Intersection.size();
		     
		     return (double) Uni / InS ;
			 } 

	 
	      @Override
	      public void reduce(TextPair key, Iterable<Text> values,
					Context context)
	              throws IOException, InterruptedException {
	    	
	    	String keyone = key.getFirst().toString();  
	    	String keytwo = key.getSecond().toString();    	
	    	 
	    	HashMap<String, String> ProcessedDoc = new HashMap<String, String>();
	    	reader = new BufferedReader(new FileReader(
								new File("/home/cloudera/workspace/MDP02Pre/rawinput.csv")));
	    	 
	    	String pattern;
				while ((pattern = reader.readLine()) != null) {
					String[] word = pattern.split(",");
					ProcessedDoc.put(word[0], word[1]);
				}
			
			HashSet<String> secondset = new HashSet<String>();
			
			String twostrings = ProcessedDoc.get(keytwo);
			for (String e : twostrings.split(" ")) {
				secondset.add(e);}
				
			HashSet<String> firstset = new HashSet<String>();
			String onestrings = ProcessedDoc.get(keyone);
			for (String f : onestrings.split(" ")) {
				firstset.add(f);}
				
			context.getCounter(UNIQUE.counter).increment(1);
			double sim = JaSim(firstset, secondset);

			if (sim >= 0.8) {
				context.write(new Text( "(" + key.toString()+ ")" ), new Text(String.valueOf(sim)) );
				}		
	      }
	   }
}

