package mdp02.pre;

import java.io.IOException;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.io.BufferedWriter;
import java.io.FileWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Collections;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MDP02Pre extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new MDP02Pre(), args);	
		System.exit(res);
		   }

	@Override
	public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "MDP02Pre");
	      job.setJarByClass(MDP02Pre.class);
	      job.setOutputKeyClass(LongWritable.class);
		  job.setOutputValueClass(Text.class); 

	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);
	      job.setNumReduceTasks(1);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);
	      	      
	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));
	      job.getConfiguration().set(
					"mapreduce.output.textoutputformat.separator", ",");
	      
	      // Delete output filepath if already exists 
	      FileSystem fs = FileSystem.newInstance(getConf());

	      if (fs.exists(new Path(args[1]))) {
				fs.delete(new Path(args[1]), true);
			}
	       
	      job.waitForCompletion(true);			
	      
	      // Print the COUNTERS Values on the CONSOLE
	      // http://www.imageterrier.org/HadoopImageTerrier/apidocs/org/terrier/indexing/HadoopIndexerMapper.Counters.html
	      Counters counters = job.getCounters();
	      Counter c1 = counters.findCounter(UNIQUE.counter);
	      
	      System.out.println("Numbers of output number is: " + c1.getValue());
	      	      
	      // http://www.codejava.net/java-se/file-io/how-to-read-and-write-text-file-in-java 
	      /*
	       * 
	        FileWriter writer = new FileWriter("MyFile.txt", true);
            BufferedWriter bufferedWriter = new BufferedWriter(writer);
 
            bufferedWriter.write("Hello World");
            bufferedWriter.newLine();
            bufferedWriter.write("See You Again!");
	       * 
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
	
		 
	 public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
		  private BufferedReader rdr;
	      @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
	    	
	    	 
	    	 HashMap<String, String> Stop = new HashMap<String, String>();
	    	 rdr = new BufferedReader(new FileReader(
								new File("/home/cloudera/workspace/MDP01D/stopWords.csv")));
	    	 
	    	 String pattern;
				while ((pattern = rdr.readLine()) != null) {
					String[] word = pattern.split(",");
					Stop.put(word[0], word[1]);
				} 
	    	// http://stackoverflow.com/questions/1625814/get-a-hashset-out-of-the-keys-of-a-hashmap
			HashSet<String> stopWords = new HashSet<String>(Stop.keySet());


	    	 for (String token: value.toString().split("\\s*\\b\\s*")) {
	        	 token = token.trim().toLowerCase();
	        	 Pattern p = Pattern.compile("^[a-zA-Z0-9_]");
		    	 Matcher m = p.matcher(token.toLowerCase());
		    	 
	        	 if (token.toLowerCase().isEmpty() 
	        			 || stopWords.contains(token.toLowerCase() ))  {
	    	                 continue;
	    	             } 
	        			 
	        	 if (!m.find() 
	        			 || value.toString().length() == 0  ) {
	                 continue;
	             } 

	             context.write(key, new Text(token.toLowerCase()));
	         }
	      }
	   }

	   public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
		   	  private BufferedReader reader;
		  
	      @Override
	      public void reduce(LongWritable key, Iterable<Text> values,
					Context context)
	              throws IOException, InterruptedException {
	    	 
	    	 ArrayList<String> title = new ArrayList<String>();
	    	 
	    	 // read in - WordCount result to be the number in the later use
	    	 // http://stackoverflow.com/questions/16246821/how-to-get-values-keys-from-hashmap
	    	 
	    	 HashMap<String, String> wordcount = new HashMap<String, String>();
	    	 reader = new BufferedReader(new FileReader(
								new File("/home/cloudera/workspace/WordCount/output/WordCount.csv")));

	    	 String pattern;
				while ((pattern = reader.readLine()) != null) {
					String[] word = pattern.split(",");
					wordcount.put(word[0], word[1]);
				}
			
	    	 for (Text num : values) {
					title.add(num.toString());
				}
	    	 
	    	 HashSet<String> ti = new HashSet<String>(title);  
	    	 StringBuilder stringBuilder = new StringBuilder();
     	 
	         for (String numi : ti) { 
	            if (numi.length() > 0);{
	        	  if (stringBuilder.length() > 0) stringBuilder.append(", ");{	        		
	        	  	stringBuilder.append(numi + "#" + wordcount.get(numi));
	        	  }
	            }
			}
	      
	         /* Internet Reference
	          * 
	          * sort() method //////////////////////////////////////////////
	          * used to sort a list according to the order induced by the specified Comparator
	          * public int compare(Student e1, Student e2){
	          *        return e1.id.compareTo(e2.id);      
	          * http://www.w3resource.com/java-tutorial/arraylist/arraylist_sort.php
	          * 
	          * 
	          * Arrays.sort
	          * LengthComparator to Arrays.sort. This orders the array.
	          * https://www.dotnetperls.com/sort-java
	          * // Sort strings by their lengths with this LengthComparator.
	          * Arrays.sort(array, new LengthComparator());
	          * 
	          * 
	          * Sorting Lists Using Comparators ////////////////////////////
	          * used to sort the elements present in the specified list of Collection in ascending order
	          * https://www.youtube.com/watch?v=QYvUmIYgsiA
	          * 
	          * List<Integer> numbers = new ArrayList<Integer>();
	          * 
	          * numbers.add(3);
	          * numbers.add(36);
	          * numbers.add(73);
	          * numbers.add(40);
	          * numbers.add(1);
	          * 
	          * Collections.sort(numbers, new Comparator<Integer>() {
	          * public int compare(Integer num1, Integer num2) {
	          * 	return num1.compareTo(num2);
	          * }
	          * 
	          * 
	          * objective here:
	          * golden#3, silver#10, red#30......
	          * 
	          * we need to remove words: "golden", "silver"....
	          * and for the hashtag as well: "#"
	          *         the space          : " "
	          * and finally use the frequency integer to sort 
	          */

	         // List<String> wordswithcountL = Arrays.asList(stringBuilder.toString().split("\\s*,\\s*")); 
	         // firstly split it by the commas, and into elements
	         // example: yellow#3001

		     List<String> rangingData = Arrays.asList(stringBuilder.toString().split("\\s*,\\s*")); 
		         // secondly utilize the collections.sort to compare the integer numbers
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
							return wo;
							} } );
	    		 
	         StringBuilder newsortedbyFreq = new StringBuilder();
	         
	         /*
	         for (String numi : rangingData) { 
		            if (numi.length() > 0);{
		        	  if (newsortedbyFreq.length() > 0) newsortedbyFreq.append(", ");{	        		
		        		  newsortedbyFreq.append(numi);
		         }
		       }
			 }
			 */
	         for (String numi : rangingData) { 
		            if (numi.length() > 0);{
		        	  if (newsortedbyFreq.length() > 0) newsortedbyFreq.append(" ");{	        		
		        		  newsortedbyFreq.append(numi.replaceAll("#\\d+", ""));
		         }
		       }
			 }
	         
	         /* Book - MapReduce Design Patterns p.160
	          * If it is empty or null, increment the NULL_OR_EMPTY_COUNTER counter by 1
	          * context.getCounter(STATE_COUNTER_GROUP, NULL_OR_EMPTY_COUNTER).increment(1);
	          * 
	          */
	         context.getCounter(UNIQUE.counter).increment(1);
	         
	         context.write(key, new Text(newsortedbyFreq.toString()
		     		.replaceAll("#\\d+", "").replaceAll(",", "").replaceAll("__#null", "")));
	      }
	   }
}
