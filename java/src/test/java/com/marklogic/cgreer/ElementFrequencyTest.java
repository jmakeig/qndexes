package com.marklogic.cgreer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class ElementFrequencyTest {

	private static Log log = LogFactory.getLog(ElementFrequencyTest.class);

	@Test
	public void runTestJob() throws Exception {

		ElementFrequency.main(new String[] {});
		
	}
}
