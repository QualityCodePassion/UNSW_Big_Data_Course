/**
 * 
 */
package comp9313.ass4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;


/**
 * This is for assignment 4 of COMP9313 - Problem 1
 * 
 * Note that the assignment request that only one file could be used
 * by each version, but I would personally prefer to put each class in its
 * own file to make it cleaner.
 * 
 * @author Tim Hale, based on the examples provided in the labs
 * @version 1.1
 */



public class ReverseGraph {
	
	/** The "Mapper" class to be used by the MapReduce that reverses the graph
	 *  by taking the "ToNodeId" to be the key and the "FromNodeId" as the value 
	 *  (which is collected by the reducer and is put on the list for the key)
	 */
	
	public static class ReverseMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
	    	StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase(),
	    			" /\t\n");

	    	Text newValue = new Text();
	    	int counter = 0;
	    	
	    	// The second value becomes the key 
			while (itr.hasMoreTokens()) {
				String node = itr.nextToken().toLowerCase();
				char c = node.charAt(0);
				
				if( node.indexOf('#') >= 0)
				{
					return;
				}
				
				// Note that I would do more error checking in a real world example
				if( ++counter == 1)
				{
					newValue.set(node);
				}
				else
				{
					word.set( node );
					context.write(word, newValue );
	
					System.out.println("Map: " + word.toString() + "," + newValue.toString() );
					//Log log = LogFactory.getLog(ReverseMapper.class);
					//log.info("Mylog@Mapper: " + word.toString());
					return;
				}
			}
		}
	}

	
	/** The "Reducer" class to be used by the MapReduce that constructs the
	 *  "ToNodeId" for each key value (which is now the "FromNodeId", after
	 *  being swapped by the Mapper)
	 */
	
	public static class ReverseReducer extends Reducer<Text, Text, Text, Text> {
		
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			String resultString = new String("\t");
			for (Text val : values) {
				resultString += val.toString() + ",";
			}
			

			result.set(resultString);
			context.write(key, result);
			
			//Log log = LogFactory.getLog(ReverseReducer.class);
			//log.info("Mylog@Reducer: " + key.toString() + result.toString());
			
			System.out.println("Reduce: " + key.toString() + result.toString());
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Reverse Graph");
	    job.setJarByClass(ReverseGraph.class);
	    job.setMapperClass(ReverseMapper.class);
	    job.setReducerClass(ReverseReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(3);
	    
	    if( args.length == 3)
		{
		    FileInputFormat.addInputPath(job, new Path(args[1]));
		    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		}
	    else
		{
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		}
	    	
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
