package comp9313.ass4;

//import ...
import java.io.IOException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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
    
    public static double NA = Double.POSITIVE_INFINITY;	// -1;
    
    private static boolean parsingInputData = true;
    private static boolean writeFinalOutput = false;
    private static long queryNodeId = -1;
    
    
    public enum UpdateCounter {
    	  CONVERGENCE_COUNTER,
    	  NODE_COUNTER
    	 }


    public static class SPMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    	
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try{
        	
				Text word = new Text();
				String line = value.toString(); // looks like 1 0 2:3:
				String[] sp = line.split("\t| "); // splits on space
				
				if( parsingInputData){
					// Read the initial input file
					word.set(sp[2] + " " + sp[3]); // ToNode Dist
					context.write(new LongWritable(Long.parseLong(sp[1])), word);
					word.clear();
				}
				else {
					// Pass through the adjacency info
					word.set("ADJ " + sp[1]);
					context.write(new LongWritable(Long.parseLong(sp[0])) , word);
					word.clear();
	
					String[] currentData = sp[1].split("=");
					double currentDist = Double.parseDouble(currentData[0]);
	
					
					if( (currentDist != NA) && (currentData.length > 1) ){
						// If current distance is still infinity, don't process this node yet
						// Otherwise, update all of the distances for all outgoing edges
						String[] edges = currentData[1].split(",");
						for( String next : edges){
							String[] edge = next.split(":");
							
							double newDist = Double.parseDouble(edge[1]);
							if( currentDist != NA ){
								// If the current distance up to the current node is a valid value
								// (i.e. set in a previous iteration), then add it to the new distance
								// for the adjacent node
								newDist += currentDist;
							}
	
							// Set the "updated" distance
							word.set("DIST " + Double.toString(newDist));
							context.write(new LongWritable(Long.parseLong(edge[0])), word);
							word.clear();
						}
					}
				}
			}
    		catch(Exception e)
    		{
    			System.out.println("Mapper caught the following exception: " + e.getMessage() );
    		}
        }
    }

    
    
    public static class SPReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	try{
        	
				if( parsingInputData){
					// If parsing the provided input data
					String outputLine;
					if( queryNodeId == key.get() ){
						outputLine = "0=";
					}
					else{
						outputLine =  Double.toString(NA) + "=";
					}
					// Read the initial input file
					for (Text val : values) { // looks like ToNodeId Distance pairs
						// need to use the first as a key
						String[] sp = val.toString().toString().split(" "); // splits on
						outputLine += sp[0] + ":" + sp[1] + ",";
					}
					
					Text word = new Text();
					word.set(outputLine);
					context.write(key, word);
				}
				else{
					String nodes = "";
					Text word = new Text();
					double lowest = Double.POSITIVE_INFINITY;
					double previousDistance = lowest;
					
					for (Text val : values) { 
						String[] sp = val.toString().toString().split(" ");
						// look at first value
						if (sp[0].equalsIgnoreCase("ADJ")) {
							// val looks like: currentMin=toNodeId:Dist,toNodeId:Dist, ... (e.g 42=2:5.0,1:10.0,)
							String[] adj = sp[1].split("=");
							double distance = Double.parseDouble(adj[0]);
							previousDistance = distance;
							lowest = Math.min(distance, lowest);
							nodes = "";
							
							if( adj.length > 1)
								nodes = adj[1];
						} 
						else if (sp[0].equalsIgnoreCase("DIST")) {
							double distance = Double.parseDouble(sp[1]);
							lowest = Math.min(distance, lowest);
						}
					}
					
					/*
					if( writeFinalOutput){
						word.set(key + " " + lowest);
						context.write(new LongWritable(queryNodeId), word);
						word.clear();
					}
					*/
					
					word.set(lowest + "=" + nodes);
					context.write(key, word);
					word.clear();
										
					// Count how many nodes were visited
				    context.getCounter(UpdateCounter.NODE_COUNTER).increment(1);

					if( lowest == previousDistance){
						// Count how many nodes have "converged"
					    context.getCounter(UpdateCounter.CONVERGENCE_COUNTER).increment(1);
					}
				}
        	}
			catch(Exception e)
			{
				System.out.println("Reducer caught the following exception: " + e.getMessage() );
			}
        }
    }
    
    

	public static void main(String[] args) throws Exception {
		
		try{
		    if( args.length == 4){
				IN = args[1];
				OUT = args[2];
				queryNodeId = Long.parseLong(args[3]);
		    }
		    else{			
				IN = args[0];
				OUT = args[1];
				queryNodeId = Long.parseLong(args[2]);
		    }

			String input = IN;
			String output = OUT + System.nanoTime();
			
			long iterationCount = 0;
			
			boolean isdone = false;
	
			while (isdone == false) {
			    Configuration conf = new Configuration();
			    conf.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
			    conf.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
			    
			    Job job = Job.getInstance(conf, "SingleSourceSP");
			    job.setJarByClass(SingleSourceSP.class);
				
			    //job.setNumReduceTasks(3);
				job.setOutputKeyClass(LongWritable.class);
				job.setOutputValueClass(Text.class);
				job.setMapperClass(SPMapper.class);
				job.setReducerClass(SPReducer.class);
			    FileInputFormat.addInputPath(job,  new Path(input));
			    FileOutputFormat.setOutputPath(job, new Path(output));
	
			    job.waitForCompletion(true);
			    
			    if( writeFinalOutput)
			    {
			    	// Once we have written the final output, we are done
			    	isdone = true;
			    	break;
			    }

			    long convergedCount =  job.getCounters().findCounter(UpdateCounter.CONVERGENCE_COUNTER)
			    	    .getValue();
			    long nodeCount =  job.getCounters().findCounter(UpdateCounter.NODE_COUNTER)
			    	    .getValue();
			    

			    // Get path to old input dir (put not the initial input) to delete it
			    Path oldInputDir = new Path(input);
				FileSystem hdfs = oldInputDir.getFileSystem( conf );

			    // delete existing input directory
			    if ( !parsingInputData && hdfs.exists(oldInputDir)) {
			        hdfs.delete(oldInputDir, true);
			    }
			    
			    if( !parsingInputData && ( ( ++iterationCount == nodeCount ) || (nodeCount == convergedCount) ) ){
			    	//set a flag to say it's the last round and let the mapper write it to required output file
			    	//writeFinalOutput = true;
			    	isdone = true;
			    }
			    
				input = output + "/part-r-00000";
			    parsingInputData = false;
			    
			    if( isdone )
			    {
			    	//FileSystem fs = FileSystem.get(conf);
			        //FileStatus[] status = fs.listStatus(new Path(IN) );
			    	
			    	// If we are done, read the last reducer output and write the output in the
			    	// desired output
					// From: http://stackoverflow.com/questions/17072543/reading-hdfs-and-local-files-in-java
			    	
			    	String filenamePrefix = new String();
			    	if( IN.contains("NA.cedge") ){
			    		filenamePrefix = "NA";
			    	}
			    	else if ( IN.contains("SF.cedge") ){
			    		filenamePrefix = "SF";
			    	}
			    	else if ( IN.contains("tiny-graph.txt") ){
			    		filenamePrefix = "tiny-graph";
			    	}
			    	else{
			    		filenamePrefix = "result-";
			    	}
			    				    	
			    	output = OUT + "/" + filenamePrefix + queryNodeId;
			    	
					Path inputPath = new Path(input);
					Path outputPath = new Path(output);
					FileSystem inputFileSystem = inputPath.getFileSystem( conf );
					FileSystem outputFileSystem = outputPath.getFileSystem( conf );
					FSDataInputStream inputStream = inputFileSystem.open(inputPath);
					
					// Delete the output dir if it already exists
				    if ( outputFileSystem.exists(outputPath)) {
				    	outputFileSystem.delete(outputPath, true);
				    }

					
					FSDataOutputStream outputStream = inputFileSystem.create(outputPath);
					 
					BufferedReader readBuffer = new BufferedReader(new InputStreamReader(
							inputStream ));
					BufferedWriter writeBuffer = new BufferedWriter(new OutputStreamWriter(
							outputStream ));

					
					String line = readBuffer.readLine();
					// Read the current output file and put it into HashMap
					while (line != null) {
						String[] sp = line.split("\t| |=");
						long node = Long.parseLong(sp[0]);
						double distance = Double.parseDouble(sp[1]);
						
						//don't ouptput any node that isn't reachable (i.e. still with "infinite" distance)
						if( distance != NA){
							String outString = queryNodeId + " " + sp[0] + " " + sp[1] + "\n";
							writeBuffer.write(outString);
						}
						line = readBuffer.readLine();
					}
					readBuffer.close();
					writeBuffer.close();
			    }
				
				/*
				isdone = true;
				
				// From: http://stackoverflow.com/questions/17072543/reading-hdfs-and-local-files-in-java
				Path path = new Path(input);
				FileSystem fs = path.getFileSystem( conf );
				FSDataInputStream inputStream = fs.open(path);
				 
				BufferedReader br = new BufferedReader(new InputStreamReader(
						inputStream ));
				
				HashMap<Long, Long> imap = new HashMap<Long, Long>();
				String line = br.readLine();
				// Read the current output file and put it into HashMap
				while (line != null) {
					String[] sp = line.split("\t| ");
					long node = Long.parseLong(sp[0]);
					long distance = Long.parseLong(sp[1]);
					imap.put(node, distance);
					line = br.readLine();
				}
				br.close();
	
				// Check for convergence condition if any node is still left then
				// continue else stop
				Iterator<Long> itr = imap.keySet().iterator();
				while (itr.hasNext()) {
					long key = itr.next();
					long value = imap.get(key);
					if (value >= 125) {
						isdone = false;
					}
				}
				*/
				
				input = output;
				
				/*
				if( writeFinalOutput){
					output = OUT + "-" + queryNodeId;
				}
				*/
				output = OUT + System.nanoTime();
			}
		}
		catch(Exception e)
		{
			System.out.println("Main function caught the following exception: " + e.getMessage() );
		}
	}
}


    
    