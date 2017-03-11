package mdp022.b;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.io.BufferedReader;
import java.io.BufferedWriter;
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
		 
	public static class Map extends Mapper<Text, Text, Text, Text> {
		  private BufferedReader rdr;
		  
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
		
		 
		 public double JaSim(HashSet<String> hs1, HashSet<String> hs2){
			 
			 Set <String> Intersection = new HashSet <String> (hs1);
			 Set <String>  Union = new HashSet <String> (hs2);
			
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
			String[] Words = twostrings.split(" ");
			long threshold_number = Math.round(Words.length - (Words.length * 0.8) + 1);
					// https://www.tutorialspoint.com/java/number_round.htm
			for (int r = 0; r < (int) threshold_number ; ++r ){
					 secondset.add(Words[r]);}

			HashSet<String> firstset = new HashSet<String>();
			String onestrings = ProcessedDoc.get(keyone);
			String[] Words_one = onestrings.split(" ");
			long threshold_numberone = Math.round(Words_one.length - (Words_one.length * 0.8) + 1);
			for (int h = 0; h < (int) threshold_numberone ; ++h ){
					 firstset.add(Words_one[h]); }
 
			double sim = 0;
			if (firstset.size() == 1){
				String fr = Words_one[0];
				if (secondset.contains(fr)) {
					sim = 1;}
				else{
					context.getCounter(UNIQUE.counter).increment(1);
					sim = JaSim(firstset, secondset); }
			}
			else if (secondset.size() == 1) {
				String cr = Words[0];
				if (firstset.contains(cr)) {
					sim = 1;}
				else{
					context.getCounter(UNIQUE.counter).increment(1);
					sim = JaSim(firstset, secondset); }
			}
			else{
			context.getCounter(UNIQUE.counter).increment(1);
			sim = JaSim(firstset, secondset); }	
			

			if (sim >= 0.8) {
				context.write(new Text( "(" + key.toString()+ ")" ), new Text(String.valueOf(sim)) );
				}		
	      }
	   }
}


