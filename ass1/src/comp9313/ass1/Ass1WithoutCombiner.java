package comp9313.ass1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
 * This is for assignment 1 of COMP9313 and is the version that doesn't
 * use a combiner.
 * 
 * Note that the assignment request that only one file could be used
 * by each version, but I would personally prefer to put each class in its
 * own file to make it cleaner.
 * 
 * @author Tim Hale, based on the examples provided in the labs
 * @version 1.1
 */



public class Ass1WithoutCombiner {
	
	/**
	 * @author Tim Hale based on the "IntPair" class in comp9313 lecture 4 notes
	 *
	 */
	public static class DoublePair implements Writable {

	    private double first, second;


	    public DoublePair() {

	    }

	    public DoublePair(double first, double second) {
	               set(first, second);
	    }


	    public void set(double left, double right) {
	               first = left;
	               second = right;
	    }


	    public double getFirst() {
	               return first;
	    }

	    public double getSecond() {
	               return second;
	    }


		@Override
	   public void write(DataOutput out) throws IOException {
	               out.writeDouble(first);
	               out.writeDouble(second);    
	    }
	    
		@Override
	   public void readFields(DataInput in) throws IOException {
	               first = in.readDouble();
	               second = in.readDouble();

	    }
	}
	
	
	
	
	
	public static class WordMapper extends Mapper<Object, Text, Text, DoublePair> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {
				String w = itr.nextToken().toLowerCase();
				String c = String.valueOf(w.charAt(0));
				
				if (Character.isLetter(c.charAt(0))){
					word.set(c);
					DoublePair nextValue = new DoublePair(w.length(), 1);
					context.write(word, nextValue );
				}
				System.out.println(word.toString());
				
				Log log = LogFactory.getLog(WordMapper.class);
				log.info("Mylog@Mapper: " + word.toString());
			}
		}
	}

	
	
	public static class WordReducer extends Reducer<Text, DoublePair, Text, DoubleWritable> {
		
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<DoublePair> values, Context context)
				throws IOException, InterruptedException {
			
			double sum = 0;
			double word_count = 0;
			for (DoublePair val : values) {
				sum += val.getFirst();
				word_count += val.getSecond();
			}
			
			double avg = sum / word_count;
			
			result.set(avg);
			context.write(key, result);
			
			Log log = LogFactory.getLog(WordReducer.class);
			log.info("Mylog@Reducer: " + key.toString() + " " + result.toString());
			
			System.out.println(key.toString() + " " + result.toString());
		}
	}
	
	
	
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "word count");
		    job.setJarByClass(Ass1WithoutCombiner.class);
		    job.setMapperClass(WordMapper.class);
		    job.setReducerClass(WordReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoublePair.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }

}
