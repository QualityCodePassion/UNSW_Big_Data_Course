package comp9313.ass4;

//import ...
import java.io.IOException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * This is for assignment 4 of COMP9313 - Problem 2
 * 
 * Modified from the "BFS unit distance SP" code provided in lecture, which is based on: 
 * https://github.com/himank/Graph-Algorithm-MapReduce/blob/master/src/DijikstraAlgo.java
 * 
 * Note that the assignment request that only one file could be used
 * by each version, but I would personally prefer to put each class in its
 * own file to make it cleaner.
 * 
 * @author Tim Hale, based on the examples provided in the labs
 * @version 1.1
 */

public class SingleSourceSP {

    public static String OUT = "output";
    public static String IN = "input";

    public static class SPMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	//public static class Map extends MapReduceBase implements
			 

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	
			Text word = new Text();
			String line = value.toString(); // looks like 1 0 2:3:
			String[] sp = line.split("\t| "); // splits on space
			int distanceadd = Integer.parseInt(sp[1]) + 1;
			String[] PointsTo = sp[2].split(":");
			for (int i = 0; i < PointsTo.length; i++) {
				word.set("VALUE " + distanceadd); // tells me to look at
													// distance value
				context.write(new LongWritable(Integer.parseInt(PointsTo[i])),
						word);
				word.clear();
			}

			// pass in current node's distance (if it is the lowest distance)
			word.set("VALUE " + sp[1]);
			context.write(new LongWritable(Integer.parseInt(sp[0])), word);
			word.clear();

			word.set("NODES " + sp[2]);
			context.write(new LongWritable(Integer.parseInt(sp[0])), word);
			word.clear();
       }
    }

    
    
    public static class SPReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
			String nodes = "UNMODED";
			Text word = new Text();
			int lowest = 125; // In this 125 is considered as infinite distance
			for (Text val : values) { // looks like NODES/VALUES 1 0 2:3:, we
										// need to use the first as a key
				String[] sp = val.toString().toString().split(" "); // splits on
																	// space
				// look at first value
				if (sp[0].equalsIgnoreCase("NODES")) {
					nodes = null;
					nodes = sp[1];
				} else if (sp[0].equalsIgnoreCase("VALUE")) {
					int distance = Integer.parseInt(sp[1]);
					lowest = Math.min(distance, lowest);
				}
			}
			word.set(lowest + " " + nodes);
			context.write(key, word);
			word.clear();
        }
}
    
    

	public static void main(String[] args) throws Exception {
		
		try{
			
			IN = args[0];
			OUT = args[1];
			String input = IN;
			String output = OUT + System.nanoTime();
			boolean isdone = false;
	
			// Reiteration again and again till the convergence
			while (isdone == false) {
				//JobConf conf = new JobConf(SingleSourceSP.class);
				//conf.setJobName("SingleSourceSP");
			    Configuration conf = new Configuration();
			    Job job = Job.getInstance(conf, "SingleSourceSP");
			    job.setJarByClass(SingleSourceSP.class);

				
			    //job.setNumReduceTasks(3);
				job.setOutputKeyClass(LongWritable.class);
				job.setOutputValueClass(Text.class);
				job.setMapperClass(SPMapper.class);
				job.setReducerClass(SPReducer.class);
			    FileInputFormat.addInputPath(job,  new Path(input));
			    FileOutputFormat.setOutputPath(job, new Path(output));
	
				//JobClient.runJob(conf);
			    job.waitForCompletion(true);
	
				input = output + "/part-r-00000";
				isdone = true;// set the job to NOT run again!
				
				// From: http://stackoverflow.com/questions/17072543/reading-hdfs-and-local-files-in-java
				Path path = new Path(input);
				FileSystem fs = path.getFileSystem( conf );
				FSDataInputStream inputStream = fs.open(path);
				 
				BufferedReader br = new BufferedReader(new InputStreamReader(
						inputStream ));
				
				HashMap<Integer, Integer> imap = new HashMap<Integer, Integer>();
				String line = br.readLine();
				// Read the current output file and put it into HashMap
				while (line != null) {
					String[] sp = line.split("\t| ");
					int node = Integer.parseInt(sp[0]);
					int distance = Integer.parseInt(sp[1]);
					imap.put(node, distance);
					line = br.readLine();
				}
				br.close();
	
				// Check for convergence condition if any node is still left then
				// continue else stop
				Iterator<Integer> itr = imap.keySet().iterator();
				while (itr.hasNext()) {
					int key = itr.next();
					int value = imap.get(key);
					if (value >= 125) {
						isdone = false;
					}
				}
				input = output;
				output = OUT + System.nanoTime();
			}
		}
		catch(Exception e)
		{
			System.out.println("Caught the following exception: " + e.getMessage() );
		}
	}
}


    
    