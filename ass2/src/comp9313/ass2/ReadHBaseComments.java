package comp9313.ass2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This is for assignment 2 (Q2) of COMP9313 and is the version that doesn't
 * use a combiner.
 * 
 * Note that the assignment request that only one file could be used
 * by each version, but I would personally prefer to put each class in its
 * own file to make it cleaner.
 * 
 * @author Tim Hale, based on the "ReadHBaseComments.java" examples provided in the labs
 * @version 1.0
 */

public class ReadHBaseComments {

	public static class AggregateMapper extends TableMapper<Text, Text> {
		public static final byte[] CF1 = "postInfo".getBytes();
		public static final byte[] CF1ATTR1 = "PostId".getBytes();
		public static final byte[] CF2 = "commentInfo".getBytes();
		public static final byte[] CF2ATTR1 = "Score".getBytes();
		public static final byte[] CF2ATTR2 = "Text".getBytes();
		public static final byte[] CF2ATTR3 = "CreationDate".getBytes();

		public static final byte[] CF3 = "userInfo".getBytes();
		public static final byte[] CF3ATTR1 = "UserId".getBytes();

		
		private Text userId = new Text();
		private Text comment = new Text();

		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws IOException, InterruptedException {
			String userIdStr = new String(value.getValue(CF3, CF3ATTR1));
			userId.set(userIdStr);
			String commentStr = new String(value.getValue(CF2, CF2ATTR2));
			comment.set(commentStr);
			
			context.write(userId, userId);	// Bit of a hack, but sending the userId as the value so it can be counted
			//context.write(userId, comment);
		}
	}

	public static class AggregateReducer extends TableReducer<Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			String usrId = key.toString();
			Integer count = 0;
			
			for (Text val : values) {
				//System.out.println("User ID = " + usrId + ", comment = " + val.toString());
				count++;
			}
			
			System.out.println("User ID = " + usrId + ", count = " + count.toString() );
			
		    Put put = new Put(Bytes.toBytes(key.toString()));
		    put.addColumn("stats".getBytes(), "count".getBytes(), Bytes.toBytes(count));
		    context.write(null, put);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration config = HBaseConfiguration.create();
		Job job = Job.getInstance(config, "ExampleRead");
		job.setJarByClass(ReadHBaseComments.class); // class that contains mapper
		
		HBaseBulkLoadComments.createTable("user_comment_stats", new String[]{"stats"} );

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
									// set other scan attrs		

		TableMapReduceUtil.initTableMapperJob(
				"comments", // input HBase table name
				scan, // Scan instance to control CF and attribute selection
				AggregateMapper.class, // mapper
				Text.class, // mapper output key
				Text.class, // mapper output value
				job);
		
		TableMapReduceUtil.initTableReducerJob(
				"user_comment_stats",        // output table
				AggregateReducer.class,    // reducer class
				job);
		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		//job.setReducerClass(AggregateReducer.class);
		
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}
}
