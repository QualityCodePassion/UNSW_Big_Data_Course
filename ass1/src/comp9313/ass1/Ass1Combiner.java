package comp9313.ass1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;


/**
 * This is for assignment 1 of COMP9313 and is the version that uses a
 * combiner, which is the only class that is different to the
 * Ass1WithoutCombiner.
 * 
 * Note that the assignment request that only one file could be used
 * by each version, but I would personally prefer to put each class in its
 * own file to make it cleaner.
 * 
 * @author Tim Hale, based on the examples provided in the labs
 * @version 1.1
 */



public class Ass1Combiner {
	
	public static class WordCombiner extends Reducer<
		Text, Ass1WithoutCombiner.DoublePair, Text, Ass1WithoutCombiner.DoublePair> {
		
		private Ass1WithoutCombiner.DoublePair result = new Ass1WithoutCombiner.DoublePair();

		public void reduce(Text key, Iterable<Ass1WithoutCombiner.DoublePair> values, Context context)
				throws IOException, InterruptedException {
			
			double sum = 0;
			double word_count = 0;
			for (Ass1WithoutCombiner.DoublePair val : values) {
				sum += val.getFirst();
				word_count += val.getSecond();
			}
			
			result.set(sum, word_count);
			context.write(key, result);
			
			Log log = LogFactory.getLog(WordCombiner.class);
			log.info("Mylog@Reducer: " + key.toString() + " " + result.toString());
			
			System.out.println(key.toString() + " " + result.toString());
		}
	}

	
	
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "word count");
		    job.setJarByClass(Ass1Combiner.class);
		    job.setMapperClass(Ass1WithoutCombiner.WordMapper.class);
		    job.setCombinerClass(WordCombiner.class);
		    job.setReducerClass(Ass1WithoutCombiner.WordReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Ass1WithoutCombiner.DoublePair.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }

}
