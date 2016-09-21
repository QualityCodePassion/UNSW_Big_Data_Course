package comp9313.ass2;


import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * This file was provided in Lab6.
 * Uses HBase's bulk load facility ({@link HFileOutputFormat2} and
 * {@link LoadIncrementalHFiles}) to efficiently load data into a
 * HBase table.
 */

public class HBaseBulkLoadComments extends Configured implements Tool {
	
	
	public static class Comment {
		String Id;
		String PostId;
		String Score;
		String Text;
		String UserId;
		String CreationDate;

		String getId() {
			return Id;
		}

		String getPostId() {
			return PostId;
		}

		String getCommentScore() {
			return Score;
		}

		String getCommentText() {
			return Text;
		}

		String getUserId() {
			return UserId;
		}

		String getCreationDate() {
			return CreationDate;
		}

		void setId(String value) {
			Id = value;
		}

		void setPostId(String value) {
			PostId = value;
		}

		void setCommentScore(String value) {
			Score = value;
		}

		void setCommentText(String value) {
			Text = value;
		}

		void setUserId(String value) {
			UserId = value;
		}

		void setCreationDate(String value) {
			CreationDate = value;
		}

		/* You can also use regular expression, e.g.:
		 * private void parse(String line){ 
		 * 		Pattern p = Pattern.compile("([a-zA-Z]+)=\"([^\\s]+)\""); 
		 * 		Matcher m = p.matcher(line); 
		 * 		while(m.find()){ System.out.println(m.group()); } 
		 * }
		 */
		
		private void parse(String line) {

			try {
				
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

				InputStream in = IOUtils.toInputStream(line, "UTF-8");
				Document doc = dBuilder.parse(in);
				
				Element ele = (Element) doc.getElementsByTagName("row").item(0);
				setId(ele.getAttribute("Id"));
				setPostId(ele.getAttribute("PostId"));
				setCommentScore(ele.getAttribute("Score"));
				setCommentText(ele.getAttribute("Text"));
				
				//if(Score.equals("5")){
				setUserId(ele.getAttribute("UserId"));
				//} else {
				//	setUserId(null);
				//}
				setCreationDate(ele.getAttribute("CreationDate"));
				
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

		public Comment(String line) {
			parse(line.trim());
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(Id + " " + PostId + " " + Score + " " + Text + " " + CreationDate + " " + UserId);
			return sb.toString();
		}
	}

	
	
	
	

	//Only the mapper is required, HBase will implement the reducer
	static class HBaseCommentMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		//in the map function, you parse the source file, and create a put object.
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Comment comment = new Comment(value.toString());
			String rowKey = comment.getId();

			System.out.println(comment.toString());

			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes("postInfo"), Bytes.toBytes("PostId"), Bytes.toBytes(comment.getPostId()));

			put.addColumn(Bytes.toBytes("commentInfo"), Bytes.toBytes("Score"), Bytes.toBytes(comment.getCommentScore()));
			put.addColumn(Bytes.toBytes("commentInfo"), Bytes.toBytes("Text"), Bytes.toBytes(comment.getCommentText()));
			put.addColumn(Bytes.toBytes("commentInfo"), Bytes.toBytes("CreationDate"), Bytes.toBytes(comment.getCreationDate()));

			put.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("UserId"), Bytes.toBytes(comment.getUserId()));

			context.write(new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put);
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.fs.tmp.dir", "/tmp/hbase-staging");
		conf.set("hbase.bulkload.staging.dir", "/tmp/hbase-staging");
		Job job = Job.getInstance(conf, "hbase bulk load");

		job.setJarByClass(getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setMapperClass(HBaseCommentMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		Connection connection = ConnectionFactory.createConnection(conf);
		TableName tableName = TableName.valueOf("comments");
		Table table = connection.getTable(tableName);

		try {

			RegionLocator locator = connection.getRegionLocator(tableName);
			HFileOutputFormat2.configureIncrementalLoad(job, table, locator);

			if (!job.waitForCompletion(true)) {
				return 1;
			}

			LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
			Admin admin = connection.getAdmin();
			loader.doBulkLoad(outputPath, admin, table, locator);
			return 0;
		} finally {
			table.close();
		}
	}

	public static void createTable(String tName, String [] CF) {
		Configuration conf = HBaseConfiguration.create();		
		try {
			Connection connection = ConnectionFactory.createConnection(conf);

			try {
				Admin admin = connection.getAdmin();
				TableName tableName = TableName.valueOf(tName);
				if(admin.tableExists(tableName)){
					return;
				}
				try {					
					HTableDescriptor htd = new HTableDescriptor(tableName);

					HColumnDescriptor[] column = new HColumnDescriptor[CF.length];
					for(int i=0;i<CF.length;i++){
						column[i] = new HColumnDescriptor(CF[i]);
						htd.addFamily(column[i]);
					}
					
					admin.createTable(htd);
				} finally {
					admin.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				connection.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		
		createTable("comments", new String[]{"postInfo", "commentInfo", "userInfo"});
		int exitCode = ToolRunner.run(HBaseConfiguration.create(), new HBaseBulkLoadComments(), args);
		System.exit(exitCode);
	}
}