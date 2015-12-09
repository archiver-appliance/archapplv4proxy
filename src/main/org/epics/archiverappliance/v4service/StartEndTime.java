package org.epics.archiverappliance.v4service;

import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAdjusters;
import java.util.HashMap;
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
		// 10 minutes ago and so on...
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
					case "months": case "month": case "mon": return ZonedDateTime.now().minusMonths(amount);
					case "years": case "year": case "y": return ZonedDateTime.now().minusYears(amount);
					default:
						throw new DateTimeParseException("Cannot parse decrements", timeStr, -1);
					}
				}
				return null;
			}
		});
		
		// 10 AM Tuesday etc..
		matchers.add(new TimeMatcher() {
			private Pattern pattern = Pattern.compile("([\\d:]+)(AM|PM) (today|yesterday|sunday|monday|tuesday|wednesday|thursday|friday|saturday|sun|mon|tue|wed|thur|thu|fri|sat)", Pattern.CASE_INSENSITIVE);
			@Override
			public ZonedDateTime parse(String timeStr) throws DateTimeParseException {
				Matcher matcher = pattern.matcher(timeStr);
				if(matcher.matches()) { 
					logger.debug("At {} ~ {} ~ {}", matcher.group(1), matcher.group(2), matcher.group(3));
					String time = matcher.group(1);
					String ampm = matcher.group(2);
					String datereference = matcher.group(3).toLowerCase();
					ZonedDateTime specifiedDateTime = null;
					switch(datereference) { 
					case "today": specifiedDateTime = ZonedDateTime.now(); break;
					case "yesterday": specifiedDateTime = ZonedDateTime.now().minusDays(1); break;
					case "sunday": case "sun": specifiedDateTime = ZonedDateTime.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY)); break;
					case "monday": case "mon": specifiedDateTime = ZonedDateTime.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)); break;
					case "tuesday": case "tue": specifiedDateTime = ZonedDateTime.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.TUESDAY)); break;
					case "wednesday": case "wed": specifiedDateTime = ZonedDateTime.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.WEDNESDAY)); break;
					case "thursday": case "thur": case "thu": specifiedDateTime = ZonedDateTime.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.THURSDAY)); break;
					case "friday": case "fri": specifiedDateTime = ZonedDateTime.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.FRIDAY)); break;
					case "saturday": case "sat": specifiedDateTime = ZonedDateTime.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.SATURDAY)); break;
					default:
						throw new DateTimeParseException("Cannot parse at", timeStr, -1);
					}
					specifiedDateTime = specifiedDateTime.with(ChronoField.MINUTE_OF_HOUR, 0).with(ChronoField.SECOND_OF_MINUTE, 0);
					String[] timeparts = time.split(":");
					int fi = 0;
					ChronoField[] fieldSequence = new ChronoField[] { ChronoField.HOUR_OF_AMPM, ChronoField.MINUTE_OF_HOUR, ChronoField.SECOND_OF_MINUTE };
					for(String timepart : timeparts) { 
						specifiedDateTime = specifiedDateTime.with(fieldSequence[fi], Long.parseLong(timepart));
						fi++;
					}
					specifiedDateTime = specifiedDateTime.with(ChronoField.AMPM_OF_DAY, ampm.equalsIgnoreCase("AM") ? 0 : 1);
					return specifiedDateTime;
				}
				return null;
			}
		});

		// ISO 8601...
		matchers.add(new TimeMatcher() {
			@Override
			public ZonedDateTime parse(String timeStr) throws DateTimeParseException {
				try { 
					DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
					ZonedDateTime dt = ZonedDateTime.parse(timeStr, formatter);
					return dt;
				} catch(DateTimeParseException ex) { 
					logger.debug("{} is not ISO 8601", timeStr);
					return null;
				}
			} });


		// Some free form formats...
		// We first break the string down by spaces
		// We look at each part and make some form of a guess as to what it could be.
		// This is the final matcher as it is the most flexible; but this can make mistakes...
		matchers.add(new TimeMatcher() {
			@Override
			public ZonedDateTime parse(String timeStr) throws DateTimeParseException {
				try {
					ZonedDateTime specifiedDateTime = ZonedDateTime.now().with(ChronoField.HOUR_OF_DAY, 0).with(ChronoField.MINUTE_OF_HOUR, 0).with(ChronoField.SECOND_OF_MINUTE, 0);
					String[] spaceParts = timeStr.split(" ");
					for(String spacePart : spaceParts) {
						if(spacePart.endsWith("AM") || spacePart.endsWith("am") || spacePart.endsWith("PM") || spacePart.endsWith("pm")) { 
							logger.debug("Parsing time specification {}", spacePart);
							if(spacePart.endsWith("PM") || spacePart.endsWith("pm")) { 
								specifiedDateTime = specifiedDateTime.with(ChronoField.AMPM_OF_DAY, 1);
							} else { 
								specifiedDateTime = specifiedDateTime.with(ChronoField.AMPM_OF_DAY, 0);
							}
							String withoutAMPM = spacePart.substring(0, spacePart.length() - 2);
							String[] timeparts = withoutAMPM.split(":");
							int fi = 0;
							ChronoField[] fieldSequence = new ChronoField[] { ChronoField.HOUR_OF_AMPM, ChronoField.MINUTE_OF_HOUR, ChronoField.SECOND_OF_MINUTE };
							for(String timepart : timeparts) { 
								specifiedDateTime = specifiedDateTime.with(fieldSequence[fi], Long.parseLong(timepart));
								fi++;
							}							
						} else { 
							try { 
								int intPart = Integer.parseInt(spacePart);
								// Yeah; kludgy I know...
								if(intPart >= 2000) { 
									logger.debug("We found the year part {}", spacePart);
									specifiedDateTime = specifiedDateTime.with(ChronoField.YEAR, intPart);
									continue;
								} else if(intPart > 0 && intPart <= 31) { 
									logger.debug("We found the day part {}", spacePart);
									specifiedDateTime = specifiedDateTime.with(ChronoField.DAY_OF_MONTH, intPart);
									continue;
								} else { 
									logger.error("Dont know what to do with int part {}", spacePart);
								}
							} catch(NumberFormatException ex) { 
								logger.debug("{} is not an integer", spacePart);
								if(month2monthId.keySet().contains(spacePart)) { 
									specifiedDateTime = specifiedDateTime.with(ChronoField.MONTH_OF_YEAR, month2monthId.get(spacePart));
								}
							}
						}
					}	
					return specifiedDateTime;
				} catch(DateTimeParseException ex) { 
					logger.debug("{} does not satisfy generic pattern 1", timeStr, ex);
					return null;
				}
			} });
	}
	
	
	/**
	 * Handle the various forms of specifying the start and end times for the request.
	 * @param timeStr
	 * @param startOrEnd
	 * @return
	 */
	public static ZonedDateTime parseDateTime(String timeStr)  throws DateTimeParseException {
		for(TimeMatcher matcher : matchers) { 
			ZonedDateTime ret = matcher.parse(timeStr);
			if(ret != null) return ret;
		}
		throw new DateTimeParseException("Cannot parse date time string ", timeStr, -1);
	}

	
	static HashMap <String, Integer> month2monthId  = new HashMap<String, Integer>();
	static { 
		month2monthId.put("January", 1);
		month2monthId.put("Jan", 1);
		month2monthId.put("February", 2);
		month2monthId.put("Feb", 2);
		month2monthId.put("March", 3);
		month2monthId.put("Mar", 3);
		month2monthId.put("April", 4);
		month2monthId.put("Apr", 4);
		month2monthId.put("May", 5);
		month2monthId.put("June", 6);
		month2monthId.put("Jun", 6);
		month2monthId.put("July", 7);
		month2monthId.put("Jul", 7);
		month2monthId.put("August", 8);
		month2monthId.put("Aug", 8);
		month2monthId.put("September", 9);
		month2monthId.put("Sep", 9);
		month2monthId.put("October", 10);
		month2monthId.put("Oct", 10);
		month2monthId.put("November", 11);
		month2monthId.put("Nov", 11);
		month2monthId.put("December", 12);
		month2monthId.put("Dec", 12);
	}
	
}
