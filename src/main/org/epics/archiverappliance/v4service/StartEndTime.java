package org.epics.archiverappliance.v4service;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Support for parsing social time specifications.
 * Currently, we support these formats.
 * 
 * @author mshankar
 *
 */
public class StartEndTime {
	private static final Logger logger = LogManager.getLogger();
	
	ZonedDateTime startTime;
	ZonedDateTime endTime;
	
	public StartEndTime(ZonedDateTime startTime, ZonedDateTime endTime) { 
		this.startTime = startTime;
		this.endTime = endTime;
	}
	
	public Instant getEndTime() {
		return getEndTime();
	}

	public Instant getStartTime() {
		return getStartTime();
	}
	
	public Timestamp getStartTimestamp() { 
		Timestamp ts = new Timestamp(this.startTime.getLong(ChronoField.INSTANT_SECONDS)*1000);
		return ts;
	}
	
	public Timestamp getEndTimestamp() { 
		Timestamp ts = new Timestamp(this.endTime.getLong(ChronoField.INSTANT_SECONDS)*1000);
		return ts;
	}

	/**
	 * Entry point into the parsing time.
	 * @param startStr
	 * @param endStr
	 * @return
	 */
	public static StartEndTime parse(String startStr, String endStr) {
		ZonedDateTime now = ZonedDateTime.now();
		ZonedDateTime startOfShift = now.minusSeconds(8*60*60);
		return new StartEndTime(startStr == null ? startOfShift : parseDateTime(startStr), endStr == null ? now : parseDateTime(endStr));
	}

	
	/**
	 * Handle the various forms of specifying the start and end times for the request.
	 * @param timeStr
	 * @param startOrEnd
	 * @return
	 */
	private static ZonedDateTime parseDateTime(String timeStr) {
		try { 
			DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
			ZonedDateTime dt = ZonedDateTime.parse(timeStr, formatter);
			return dt;
		} catch(DateTimeParseException ex) { 
			logger.error("Exception parsing {}", timeStr, ex);
			throw ex;
		}
	}

}
