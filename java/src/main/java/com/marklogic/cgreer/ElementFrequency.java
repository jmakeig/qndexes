package com.marklogic.cgreer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.filter.ElementFilter;
import org.jdom.input.DOMBuilder;
import org.jdom.input.SAXBuilder;
import org.jdom.output.DOMOutputter;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.w3c.dom.Document;

import com.marklogic.mapreduce.DocumentInputFormat;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicNode;
import com.marklogic.mapreduce.PropertyOutputFormat;

/**
 * Count the frequency of occurrences of link titles in documents in MarkLogic
 * Server, and write a link count summary to HDFS. Use with the configuration
 * file conf/marklogic-advanced.xml.
 */
public class ElementFrequency {


	/* Here are Axes that we're interested in collecting aggregates for: */
	static String documentKeyPrefix = "ROOT:";
	static String typeKeyPrefix = "TYPE:";


	private static Log log = LogFactory.getLog(ElementFrequency.class);

	
	public static class ElementFrequencyMapper extends
			Mapper<DocumentURI, MarkLogicNode, Text, Text> {
		public static final Log log = LogFactory
				.getLog(ElementFrequencyMapper.class);

		
		private String getKey(Element e) {
			return e.getNamespaceURI() + e.getName();
		}

		public void map(DocumentURI key, MarkLogicNode value, Context context)
				throws IOException, InterruptedException {
			if (key != null && value != null && value.get() != null) {
				try {
					org.jdom.Document doc = new DOMBuilder().build((Document) value.get());
					//log.info("Processing key "+key.toString());
					
					java.util.Iterator iterator=  doc.getDescendants(new ElementFilter());
					
					while (iterator.hasNext()) {
						Element e = (Element) iterator.next();
						String elementKey = getKey(e);
						String hadoopKey = key + elementKey;
						//log.info(e.getName());

						Element metaElement = new Element("element");
						metaElement = metaElement.setAttribute("frequency", "1");
						metaElement = metaElement.setAttribute("documentUri", key.toString());
						metaElement = metaElement.setAttribute("ns", e.getNamespace().getURI());
						metaElement = metaElement.setAttribute("localname", e.getName());
						metaElement = metaElement.addContent(new org.jdom.Text(elementKey));
						
						XMLOutputter out = new XMLOutputter(Format.getPrettyFormat());
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						out.output(metaElement, baos);
						String outputString = baos.toString();
						//log.info(outputString);
						context.write(new Text(hadoopKey), new Text(outputString));
						
						if (e.getParentElement() != null) {
							Element pcElement = new Element("element-element");
							pcElement = pcElement.setAttribute("frequency", "1");
							pcElement = pcElement.setAttribute("documentUri", key.toString());
							pcElement = pcElement.setAttribute("ns2", e.getNamespaceURI());
							pcElement = pcElement.setAttribute("localname2", e.getName());
							pcElement = pcElement.setAttribute("ns1", e.getParentElement().getNamespaceURI());
							pcElement = pcElement.setAttribute("localname1", e.getParentElement().getName());
							pcElement = pcElement.addContent(new org.jdom.Text(getKey(e.getParentElement()) + elementKey));
						}
						
						
						
					}
					
				}
				catch (ClassCastException e) {
					e.printStackTrace();
					log.warn("Didn't get a document node from document uri " + key.toString());

				} 
				
			} else {
				log.error("key: " + key + ", value: " + value);
			}
		}
	}

	public static class ElementFrequencyReducer extends
			Reducer<Text, Text, DocumentURI, MarkLogicNode> {
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			log.debug("Reducing key "+key.toString());
			
			SAXBuilder builder = new SAXBuilder();
			int frequency = 0;
			Element e = null;
			for (Text val : values) {
				try {
					e = builder.build(new ByteArrayInputStream(val.toString().getBytes())).getRootElement();

					frequency += Integer.parseInt(e.getAttributeValue("frequency").toString());
				} catch (JDOMException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

			DocumentURI documentUri = new DocumentURI(e.getAttributeValue("documentUri"));
			Element e2 = new Element(e.getName());
			e2.setAttribute("frequency", Integer.toString(frequency));
			e2.setAttribute("ns", e.getAttributeValue("ns"));
			e2.setAttribute("localname",e.getAttributeValue("localname"));
			
			org.jdom.Document d = new org.jdom.Document(e2);
			
			// this section just for logging
			XMLOutputter out = new XMLOutputter(Format.getPrettyFormat());
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			out.output(d, baos);
			String outputString = baos.toString();
			log.info(outputString);
			// end logging
			org.w3c.dom.Document w3cDoc;
			try {
				w3cDoc = new DOMOutputter().output(d);

				MarkLogicNode outputElement = new MarkLogicNode(w3cDoc);
				
				log.info("Writing " + outputElement.toString() + " to "+documentUri.toString());
				context.write(documentUri, outputElement);
			} catch (JDOMException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

		}

	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		
		Configuration conf = new Configuration();

		Job job = new Job(conf);
		job.setJarByClass(ElementFrequency.class);

		// Map related configuration
		job.setInputFormatClass(DocumentInputFormat.class);
		job.setMapperClass(ElementFrequencyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Reduce related configuration
		job.setReducerClass(ElementFrequencyReducer.class);
		job.setOutputFormatClass(PropertyOutputFormat.class);
		job.setOutputKeyClass(DocumentURI.class);
		job.setOutputValueClass(MarkLogicNode.class);

		conf = job.getConfiguration();
		conf.addResource("x.xml");
		log.debug("Starting job with configuration " + conf.toString());
        System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
