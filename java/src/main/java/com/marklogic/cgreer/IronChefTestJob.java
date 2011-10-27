package com.marklogic.cgreer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Document;

import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicNode;

/**
 * Count the frequency of occurrences of link titles in documents in
 * MarkLogic Server, and write a link count summary to HDFS. 
 * Use with the configuration file conf/marklogic-advanced.xml.
 */
public class IronChefTestJob {
	
	private static Log log = LogFactory.getLog(IronChefTestJob.class);
	
	 public static class RefMapper 
	    extends Mapper<DocumentURI, MarkLogicNode, IntWritable, Text> {
	        public static final Log log =
	            LogFactory.getLog(RefMapper.class);
	        private final static IntWritable one = new IntWritable(1);
	        private Text firstWord = new Text();
	        
	        public void map(DocumentURI key, MarkLogicNode value, Context context) 
	        throws IOException, InterruptedException {
	            if (key != null && value != null && value.get() != null) {
	                // grab the first word from the document text
	                Document doc = (Document) value.get();
	                
	                String text = doc.getDocumentElement().getNodeName();
	                firstWord.set(text);
	                context.write(one, firstWord);
	            } else {
	                log.error("key: " + key + ", value: " + value);
	            }
	        }
	    }

    public static class DocumentSummaryReducer
    extends Reducer<IntWritable, Text, DocumentURI, Text> {
        private Text result = new Text();
        private DocumentURI outputURI =
            new DocumentURI("result.txt");
        private String allWords = new String();

        public void reduce(IntWritable key, Iterable<Text> values,
                Context context
                ) throws IOException, InterruptedException {
            // Sort the words
            ArrayList<String> words = new ArrayList<String>();
            for (Text val : values) {
                words.add(val.toString());
            }
            Collections.sort(words);

            // concatenate the sorted words into a single string
            allWords = "";
            Iterator<String> iter = words.iterator();
            while (iter.hasNext()) {
                allWords += iter.next() + " ";
            }

            // save the final result
            result.set(allWords.trim());
            context.write(outputURI, result);

        }

    }

}
