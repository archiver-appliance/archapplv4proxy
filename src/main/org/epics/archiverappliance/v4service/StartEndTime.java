package org.epics.archiverappliance.v4service;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
	public static StartEndTime parse(String startStr, String endStr) throws DateTimeParseException {
		ZonedDateTime now = ZonedDateTime.now();
		ZonedDateTime startOfShift = now.minusSeconds(8*60*60);
		
		if(startStr == null && endStr == null) { 
			return new StartEndTime(startOfShift, now);
		}
		
		// First parse the end time...
		ZonedDateTime end = now;
		if(endStr != null) {
			end = parseDateTime(endStr); 
		}
		
		ZonedDateTime start = end.minusSeconds(8*60*60);
		if(startStr != null) { 
			start = parseDateTime(startStr);
		}

		return new StartEndTime(start, end);
	}

	
	/**
	 * Each type of time entity we support has a matcher...
	 * @author mshankar
	 *
	 */
	static interface TimeMatcher { 
		public ZonedDateTime parse(String timeStr) throws DateTimeParseException;
	}
	
	/**
	 * A regex pattern and a function that parses the supplied string
	 * @author mshankar
	 *
	 */
	static class PatternedMatcher implements TimeMatcher { 
		Pattern pattern;
		Function<String,ZonedDateTime> parser;
		public PatternedMatcher(String regexPattern, Function<String, ZonedDateTime> parser) {
			this.pattern = Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE);
			this.parser = parser;
		}
		@Override
		public ZonedDateTime parse(String timeStr) {
			if(pattern.matcher(timeStr).matches()) { 
				return parser.apply(timeStr);
			} else { 
				return null;
			}
		}
	}
	
	
	static List<TimeMatcher> matchers = new LinkedList<TimeMatcher>();
	static { 
		matchers.add(new PatternedMatcher("now", (timeStr) -> ZonedDateTime.now()));
		matchers.add(new PatternedMatcher("yesterday", (timeStr) -> ZonedDateTime.now().minusDays(1)));
		matchers.add(new TimeMatcher() {
			private Pattern pattern = Pattern.compile("(\\d+) (\\w+) ago", Pattern.CASE_INSENSITIVE);
			@Override
			public ZonedDateTime parse(String timeStr) throws DateTimeParseException {
				Matcher matcher = pattern.matcher(timeStr);
				if(matcher.matches()) { 
					logger.debug("Decrementing {} {}", matcher.group(1), matcher.group(2));
					Integer amount = new Integer(matcher.group(1));
					String units = matcher.group(2);
					switch(units) { 
					case "minutes": case "minute": case "min": case "m": return ZonedDateTime.now().minusMinutes(amount);
					case "hours": case "hour": case "h": return ZonedDateTime.now().minusHours(amount);
					case "days": case "day": case "d": return ZonedDateTime.now().minusDays(amount);
					case "weeks": case "week": case "w": return ZonedDateTime.now().minusDays(7*amount);
					case "months": case "mon": return ZonedDateTime.now().minusMonths(amount);
					case "years": case "year": case "y": return ZonedDateTime.now().minusYears(amount);
					default:
						throw new DateTimeParseException("Cannot parse decrements", timeStr, -1);
					}
				}
				return null;
			}
		});
		
		
		
		// Final matcher
		matchers.add(new TimeMatcher() {
			@Override
			public ZonedDateTime parse(String timeStr) throws DateTimeParseException {
				try { 
					DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
					ZonedDateTime dt = ZonedDateTime.parse(timeStr, formatter);
					return dt;
				} catch(DateTimeParseException ex) { 
					logger.error("Exception parsing {}", timeStr, ex);
					throw ex;
				}
			} });
	}
	
	
	/**
	 * Handle the various forms of specifying the start and end times for the request.
	 * @param timeStr
	 * @param startOrEnd
	 * @return
	 */
	private static ZonedDateTime parseDateTime(String timeStr)  throws DateTimeParseException {
		for(TimeMatcher matcher : matchers) { 
			ZonedDateTime ret = matcher.parse(timeStr);
			if(ret != null) return ret;
		}
		throw new DateTimeParseException("Cannot parse date time string ", timeStr, -1);
	}

}
