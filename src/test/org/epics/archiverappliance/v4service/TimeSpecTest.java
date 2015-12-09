package org.epics.archiverappliance.v4service;

import static org.junit.Assert.assertTrue;

import java.time.DayOfWeek;
import java.time.ZoneId;
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
		int currentYear = ZonedDateTime.now().get(ChronoField.YEAR);
		ZoneId currentZoneId = ZonedDateTime.now().getZone();

		tests.add(new TimeTest("now", ZonedDateTime.now(), 5));
		tests.add(new TimeTest("yesterday", ZonedDateTime.now().minusDays(1), 5));
		tests.add(new TimeTest("10 minutes ago", ZonedDateTime.now().minusMinutes(10), 5));
		tests.add(new TimeTest("7 days ago", ZonedDateTime.now().minusDays(7), 5));
		tests.add(new TimeTest("4 weeks ago", ZonedDateTime.now().minusDays(28), 5));
		tests.add(new TimeTest("1 month ago", ZonedDateTime.now().minusMonths(1), 5));
		// ISO 8601
		tests.add(new TimeTest("2011-12-03T10:15:30+01:00", ZonedDateTime.of(2011, 12, 03, 10, 15, 30, 0, ZoneId.of("+01:00")), 0));
		tests.add(new TimeTest("2011-12-03T10:15:30-07:00", ZonedDateTime.of(2011, 12, 03, 10, 15, 30, 0, ZoneId.of("-07:00")), 0));
		tests.add(new TimeTest("2011-12-03T10:15:30Z", ZonedDateTime.of(2011, 12, 03, 10, 15, 30, 0, ZoneId.of("Z")), 0));
		tests.add(new TimeTest("2011-12-03T10:15:30.000Z", ZonedDateTime.of(2011, 12, 03, 10, 15, 30, 0, ZoneId.of("Z")), 0));
		
		tests.add(new TimeTest("10AM today", ZonedDateTime.now().with(ChronoField.HOUR_OF_DAY, 10).with(ChronoField.MINUTE_OF_HOUR, 0).with(ChronoField.SECOND_OF_MINUTE, 0), 0));
		tests.add(new TimeTest("11PM Monday", ZonedDateTime.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)).with(ChronoField.HOUR_OF_DAY, 23).with(ChronoField.MINUTE_OF_HOUR, 0).with(ChronoField.SECOND_OF_MINUTE, 0), 0));

		// Free form formats.
		tests.add(new TimeTest("Feb 10", ZonedDateTime.of(currentYear, 2, 10, 0, 0, 0, 0, currentZoneId), 0));
		tests.add(new TimeTest("Feb 10 2014", ZonedDateTime.of(2014, 2, 10, 0, 0, 0, 0, currentZoneId), 0));
		tests.add(new TimeTest("10 Feb 2014", ZonedDateTime.of(2014, 2, 10, 0, 0, 0, 0, currentZoneId), 0));
		tests.add(new TimeTest("Feb 10 11AM", ZonedDateTime.of(currentYear, 2, 10, 11, 0, 0, 0, currentZoneId), 0));
		tests.add(new TimeTest("Feb 10 11:15AM", ZonedDateTime.of(currentYear, 2, 10, 11, 15, 0, 0, currentZoneId), 0));
		tests.add(new TimeTest("Feb 10 11:15:43AM", ZonedDateTime.of(currentYear, 2, 10, 11, 15, 43, 0, currentZoneId), 0));
		tests.add(new TimeTest("Feb 10 11:15:43PM", ZonedDateTime.of(currentYear, 2, 10, 23, 15, 43, 0, currentZoneId), 0));
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
