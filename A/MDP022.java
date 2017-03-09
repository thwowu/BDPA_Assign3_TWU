package mdp022;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
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
    
	      job.setMapOutputKeyClass(Text.class);	      
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
		 
	public static class Map extends Mapper<Text, Text, Text, Text> {
		  private BufferedReader rdr;;
		  
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
				
				StringBuilder stringBuilder = new StringBuilder();
				
				int wo = Integer.parseInt(Keystring);
				int lineline = Integer.parseInt(line);
				
				if ( wo > lineline ) {  // 1 vs 2: write ; 1 vs 3: write ; 2 vs 1: continue ; 3 vs 1: continue
					continue;}
				
				stringBuilder.append(Keystring + "," +line);
				context.write(new Text(stringBuilder.toString()), new Text(value.toString() ) );
				
			}
	     }
	}
	   

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		 private BufferedReader reader;
		 
		 /* inspired by this blog, originally from C# code 
		  * https://nickgrattan.wordpress.com/2014/02/18/jaccard-similarity-index-for-measuring-document-similarity/
		  * 
		  * public double Cal(HashSet<String> hs1, HashSet<String> hs2){
		  *     return ((double)hs1.Intersect(hs2).Count() / (double)hs1.Union(hs2).Count());}
		  *     
		  */
		 
		 public double JaSim(HashSet<String> hs1, HashSet<String> hs2){
			 
			 Set <String> Intersection = new HashSet <String> (hs1);
			 Set <String> Union = new HashSet <String> (hs2);
			 // http://stackoverflow.com/questions/8882097/is-there-a-way-to-calculate-the-intersection-of-two-sets
				 
			 Intersection.retainAll(hs2); // hs1 intersects with hs2 
			 Union.addAll(hs1); // hs2 +++ hs1 is the union *(putting all together without duplicate)
		     
		     int InS = Intersection.size();
		     int Uni = Union.size();
		   
		     return (double) InS / Uni ;
			 } 

	 
	      @Override
	      public void reduce(Text key, Iterable<Text> values,
					Context context)
	              throws IOException, InterruptedException {
	    	
	    	String[] ke = key.toString().split(",");
	    	String keyone = ke[0];  
	    	String keytwo = ke[1];    	
	    	 
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

