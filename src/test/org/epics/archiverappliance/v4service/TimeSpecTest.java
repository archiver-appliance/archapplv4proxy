package org.epics.archiverappliance.v4service;

import static org.junit.Assert.assertTrue;

import java.time.DayOfWeek;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAdjusters;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the various ways of specifying time.
 * @author mshankar
 *
 */
public class TimeSpecTest {
	private static final Logger logger = LogManager.getLogger();
	private class TimeTest { 
		String timeSpecification;
		ZonedDateTime expectedTime;
		public TimeTest(String timeSpecification, ZonedDateTime expectedTime, long toleranceInSeconds) {
			this.timeSpecification = timeSpecification;
			this.expectedTime = expectedTime;
			this.toleranceInSeconds = toleranceInSeconds;
		}
		long toleranceInSeconds;
	}
	
	private List<TimeTest> tests = new LinkedList<TimeTest>();

	@Before
	public void setUp() throws Exception {
		tests.add(new TimeTest("now", ZonedDateTime.now(), 5));
		tests.add(new TimeTest("yesterday", ZonedDateTime.now().minusDays(1), 5));
		tests.add(new TimeTest("10 minutes ago", ZonedDateTime.now().minusMinutes(10), 5));
		tests.add(new TimeTest("7 days ago", ZonedDateTime.now().minusDays(7), 5));
		tests.add(new TimeTest("4 weeks ago", ZonedDateTime.now().minusDays(28), 5));
		tests.add(new TimeTest("1 month ago", ZonedDateTime.now().minusMonths(1), 5));
		tests.add(new TimeTest("10AM today", ZonedDateTime.now().with(ChronoField.HOUR_OF_DAY, 10).with(ChronoField.MINUTE_OF_HOUR, 0).with(ChronoField.SECOND_OF_MINUTE, 0), 0));
		tests.add(new TimeTest("11PM Monday", ZonedDateTime.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)).with(ChronoField.HOUR_OF_DAY, 23).with(ChronoField.MINUTE_OF_HOUR, 0).with(ChronoField.SECOND_OF_MINUTE, 0), 0));
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testTimeSpecifications() {
		DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
		for(TimeTest timeTest : tests) { 
			logger.debug("Testing " + timeTest.timeSpecification);
			ZonedDateTime parsedTime = StartEndTime.parseDateTime(timeTest.timeSpecification);
			assertTrue("Cannot seem to parse " +  timeTest.timeSpecification, parsedTime != null);
			assertTrue("Parsing " + timeTest.timeSpecification + " yields time out of range " + parsedTime.format(formatter), 
					Math.abs(parsedTime.getLong(ChronoField.INSTANT_SECONDS) - timeTest.expectedTime.getLong(ChronoField.INSTANT_SECONDS)) <= timeTest.toleranceInSeconds);
		}
	}

}
