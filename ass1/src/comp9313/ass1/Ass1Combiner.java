package comp9313.ass1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;

import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.Writable;





public class Ass1Combiner {
	
	/**
	 * @author From comp9313 lecture 4 notes
	 *
	 */
	public static class FloatPair implements Writable {

	    private float first, second;


	    public FloatPair() {

	    }

	    public FloatPair(float first, float second) {
	               set(first, second);
	    }


	    public void set(float left, float right) {
	               first = left;
	               second = right;
	    }


	    public float getFirst() {
	               return first;
	    }

	    public float getSecond() {
	               return second;
	    }


		@Override
	   public void write(DataOutput out) throws IOException {
	               out.writeFloat(first);
	               out.writeFloat(second);    
	    }

	    
		@Override
	   public void readFields(DataInput in) throws IOException {
	               first = in.readFloat();
	               second = in.readFloat();

	    }
	}

	
	
	
	
	
	public static class WordMapper extends Mapper<Object, Text, Text, FloatPair> {

		//private final static FloatPair one = new FloatPair(1,2);
		//private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {
				String w = itr.nextToken().toLowerCase();
				String c = String.valueOf(w.charAt(0));
				
				if (Character.isLetter(c.charAt(0))){
					word.set(c);
					FloatPair nextValue = new FloatPair(w.length(), 1);
					context.write(word, nextValue );
				}
				System.out.println(word.toString());
				
				Log log = LogFactory.getLog(WordMapper.class);
				log.info("Mylog@Mapper: " + word.toString());
			}
		}
	}

	
	public static class WordReducer extends Reducer<Text, FloatPair, Text, FloatWritable> {
		
		private FloatWritable result = new FloatWritable();

		public void reduce(Text key, Iterable<FloatPair> values, Context context)
				throws IOException, InterruptedException {
			
			float sum = 0;
			float word_count = 0;
			for (FloatPair val : values) {
				sum += val.getFirst();
				word_count += val.getSecond();
			}
			
			float avg = sum / word_count;
			
			result.set(avg);
			context.write(key, result);
			
			Log log = LogFactory.getLog(WordReducer.class);
			log.info("Mylog@Reducer: " + key.toString() + " " + result.toString());
			
			System.out.println(key.toString() + " " + result.toString());
		}
	}

	
	
	public static class WordCombiner extends Reducer<Text, FloatPair, Text, FloatPair> {
		
		private FloatPair result = new FloatPair();

		public void reduce(Text key, Iterable<FloatPair> values, Context context)
				throws IOException, InterruptedException {
			
			float sum = 0;
			float word_count = 0;
			for (FloatPair val : values) {
				sum += val.getFirst();
				word_count += val.getSecond();
			}
			
			//float avg = sum / word_count;
			
			result.set(sum, word_count);
			context.write(key, result);
			
			Log log = LogFactory.getLog(WordReducer.class);
			log.info("Mylog@Reducer: " + key.toString() + " " + result.toString());
			
			System.out.println(key.toString() + " " + result.toString());
		}
	}

	
	
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "word count");
		    job.setJarByClass(Ass1Combiner.class);
		    job.setMapperClass(WordMapper.class);
		    job.setCombinerClass(WordCombiner.class);
		    job.setReducerClass(WordReducer.class);
//		    job.setCombinerClass(Ass1WithoutCombiner.WordReducer.class);
//		    job.setReducerClass(Ass1WithoutCombiner.WordReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(FloatPair.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }

}
