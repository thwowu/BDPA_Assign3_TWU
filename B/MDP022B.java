package mdp022.b;

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
					
			@Override
				public int hashCode() {
				return first.hashCode() * 163 + second.hashCode();
				}	
			
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
		
			@Override 
				public int compareTo(TextPair late){
				
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


public class MDP022B extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new MDP022B(), args);	
		System.exit(res);
		   }

	@Override
	public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "MDP022B");
	      job.setJarByClass(MDP022B.class);
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
	    	 
	    	HashSet<String> Di = new HashSet<String>();
	    	rdr = new BufferedReader(new FileReader(
								new File("/home/cloudera/workspace/MDP02Pre/rawinput.csv")));
	    	
	    	String pattern;
				while ((pattern = rdr.readLine()) != null) {
					String[] word = pattern.split(",");
					Di.add(word[0]);
				} 
			
			String ID = key.toString();
			for (String line : Di) {

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
		
		 
		 public double JaSim(HashSet<String> hs1, HashSet<String> hs2){
			 
			 HashSet <String> Intersection = new HashSet <String> (hs1);
			 HashSet <String> Union = new HashSet <String> (hs2);
			
			 Intersection.retainAll(hs2); // hs1 intersects with hs2 
			 Union.addAll(hs1); // hs2 +++ hs1 is the union *(putting all together without duplicate)
		     
		     int Uni = Union.size();
		     int InS = Intersection.size();
		     
		     return (double) InS / Uni ;
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
				 String[] Words = twostrings.split(" ");
				 long threshold_number = Math.round(Words.length - (Words.length * 0.8) + 1);
					// https://www.tutorialspoint.com/java/number_round.htm
				 String[] wordstokeepL = Arrays.copyOfRange(Words, 0,
							(int) threshold_number);
				 for (String wordtokeep : wordstokeepL) {
					 secondset.add(wordtokeep);}

				 
				 HashSet<String> firstset = new HashSet<String>();
				 
				 String onestrings = ProcessedDoc.get(keyone);
				 String[] Words_one = onestrings.split(" ");
				 long threshold_numberone = Math.round(Words_one.length - (Words_one.length * 0.8) + 1);
					// https://www.tutorialspoint.com/java/number_round.htm
				 String[] wordstok = Arrays.copyOfRange(Words, 0,
							(int) threshold_numberone);
				 for (String wordtokee : wordstok) {
					 firstset.add(wordtokee);}
				 
				 
				 /*
				  * alternative solution
				  * 
				  * 
					Set<String> stringsSet = new HashSet<>();
					HashSet<String> secondset = new HashSet<>();
					
					String twostrings = ProcessedDoc.get(keytwo);
					for (String e : twostrings.split(" ")) {
						stringsSet.add(e);
						secondset.add(e);}

					List<String> stringsList = new ArrayList<>(stringsSet);
					
					int setsizeOne = stringsSet.size();
					long threshold_numberTwo = Math.round( setsizeOne - ( setsizeOne * 0.8) + 1);
					int numforloopOne = (int) threshold_numberTwo;
					for (int i = numforloopOne; i < setsizeOne; ++i){
						secondset.remove(stringsList.get(i));}
					
					
					Set<String> stringsSet2 = new HashSet<>();
					HashSet<String> firstset = new HashSet<String>();
					
					String onestrings = ProcessedDoc.get(keyone);
					for (String f : onestrings.split(" ")) {
						firstset.add(f);
						stringsSet2.add(f);}		
					
					List<String> stringsList2 = new ArrayList<>(stringsSet2);
					
					int setsizeTwo = stringsSet2.size();
					long threshold_numberThree = Math.round( setsizeTwo - ( setsizeTwo * 0.8) + 1);
					int numforloopTwo = (int) threshold_numberThree ;
					for (int ki = numforloopTwo; ki < setsizeTwo; ++ki){
						firstset.remove(stringsList2.get(ki));}
					
					*/
				 
			context.getCounter(UNIQUE.counter).increment(1);
			double sim = JaSim(firstset, secondset);

			if (sim >= 0.8) {
				context.write(new Text( "(" + key.toString()+ ")" ), new Text(String.valueOf(sim)) );
				}		
	      }
	   }
}


