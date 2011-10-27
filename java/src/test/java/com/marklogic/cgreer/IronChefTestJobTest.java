package com.marklogic.cgreer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import com.marklogic.cgreer.IronChefTestJob.DocumentSummaryReducer;
import com.marklogic.cgreer.IronChefTestJob.RefMapper;
import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.DocumentInputFormat;
import com.marklogic.mapreduce.DocumentURI;

public class IronChefTestJobTest {

	private static Log log = LogFactory.getLog(IronChefTestJobTest.class);

	@Test
	public void runTestJob() throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf);
		job.setJarByClass(IronChefTestJob.class);

		// Map related configuration
		job.setInputFormatClass(DocumentInputFormat.class);
		job.setMapperClass(RefMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		// Reduce related configuration
		job.setReducerClass(DocumentSummaryReducer.class);
		job.setOutputFormatClass(ContentOutputFormat.class);
		job.setOutputKeyClass(DocumentURI.class);
		job.setOutputValueClass(Text.class);

		conf = job.getConfiguration();
		conf.addResource("x.xml");
		log.debug("Starting job with configuration " + conf.toString());

		job.waitForCompletion(true);
	}
}
