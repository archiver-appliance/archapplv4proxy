/**
 * helloWorld is meant to illustrate the simplest example of a remote procedure call (RPC)
 * style interaction between a client and a server using EPICS V4. 
 */
package org.epics.archiverappliance.v4service;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import gov.aps.jca.CAException;

import java.sql.Timestamp;
import java.text.ParseException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.retrieval.client.EpicsMessage;
import org.epics.archiverappliance.retrieval.client.GenMsgIterator;
import org.epics.archiverappliance.retrieval.client.InfoChangeHandler;
import org.epics.archiverappliance.retrieval.client.RawDataRetrieval;
import org.epics.pvaccess.PVAException;
import org.epics.pvaccess.server.rpc.RPCRequestException;
import org.epics.pvaccess.server.rpc.RPCServer;
import org.epics.pvaccess.server.rpc.RPCService;
import org.epics.pvdata.factory.FieldFactory;
import org.epics.pvdata.factory.PVDataFactory;
import org.epics.pvdata.pv.Field;
import org.epics.pvdata.pv.FieldCreate;
import org.epics.pvdata.pv.PVDoubleArray;
import org.epics.pvdata.pv.PVIntArray;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.Status.StatusType;
import org.epics.pvdata.pv.Structure;

/**
 * V4ArchApplProxy is a proxy to the archiver appliance
 * 
 * @author Murali Shankar (mshankar@slac.stanford.edu)
 */
public class V4ArchApplProxy
{

	// All EPICS V4 services return PVData objects (by definition). Create the
	// factory object that will allow you to create the returned PVData object
	// later.
	//
	private static final FieldCreate fieldCreate = FieldFactory.getFieldCreate();
	private static Logger logger = LogManager.getLogger();

	/**
	 * Implementation of RPC service.
	 */
	static class HelloServiceImpl implements RPCService
	{
		private String serverRetrievalURL;
		
		public HelloServiceImpl(String serverRetrievalURL) { 
			this.serverRetrievalURL = serverRetrievalURL;
		}
		
		public PVStructure request(PVStructure args) throws RPCRequestException
		{
			PVStructure query = args.getStructureField("query");
			String pvName = query.getStringField("pv").get();
			String start = query.getStringField("from").get();
			String end = query.getStringField("to").get();
			
			logger.debug("Getting data for pv {} from {} to {}", pvName, start, end);
			
            try { 
            	FetchDataFromAppliance fetchData = new FetchDataFromAppliance(this.serverRetrievalURL, pvName, start, end);
            	PVStructure result = fetchData.getData();
            	return result;
            } catch(Exception ex) { 
            	throw new RPCRequestException(StatusType.ERROR, "Exception fetching data", ex);
            }
		}
	}

	/**
	 * Main is the entry point of the HelloService server side executable. 
	 * @param args None
	 * @throws CAException
	 */
	public static void main(String[] args) throws PVAException
	{
		if(args.length < 2) { 
			System.err.println("Usage: java org.epics.archiverappliance.v4service.V4ArchApplProxy <serviceName> <serverRetrievalURL>");
			System.err.println("For example: java org.epics.archiverappliance.v4service.V4ArchApplProxy archProxy http://archiver.facility.org/retrieval/data/getData.raw");
			return;
		}
		
		String serviceName = args[0];
		String serverRetrievalURL = args[1];
		
		RPCServer server = new RPCServer();

		// Register the service
		server.registerService(serviceName, new HelloServiceImpl(serverRetrievalURL));
		
		logger.info("Starting the EPICS archiver appliance proxy under the service name {} proxying the server {}", serviceName, serverRetrievalURL);
		

		server.printInfo();
		server.run(0);
	}

	private static class FetchDataFromAppliance implements InfoChangeHandler  {
		String pvName;
		Timestamp start;
		Timestamp end;
		String serverURL;
		
		public FetchDataFromAppliance(String serverDataRetrievalURL, String pvName, String startStr, String endStr) throws ParseException {
			this.serverURL = serverDataRetrievalURL;
			this.pvName = pvName;
			this.start = parseDateTime(startStr, TimeParsing.START);
			this.end = parseDateTime(endStr, TimeParsing.END);
			logger.debug("Getting data for pv {} from {} to {}", pvName, start.getTime(), end.getTime());
		}

		public PVStructure getData() throws Exception {
			try {
				logger.debug("Making a call");
				// Create the result structure of the data interface.
	            String[] columnNames = new String[]{"epochSeconds", "values", "nanos", "severity", "status"};

	            Structure valueStructure = fieldCreate.createStructure(
	            		columnNames,
	            		 new Field[] {
	                            fieldCreate.createScalarArray(ScalarType.pvDouble),
	                            fieldCreate.createScalarArray(ScalarType.pvDouble),
	                            fieldCreate.createScalarArray(ScalarType.pvDouble),
	                            fieldCreate.createScalarArray(ScalarType.pvInt),
	                            fieldCreate.createScalarArray(ScalarType.pvInt),
	                            }
	            		);
	            Structure resultStructure =
	                    fieldCreate.createStructure( "epics:nt/NTTable:1.0",
	                            new String[] { "labels", "value" },
	                            new Field[] { 
	                    			fieldCreate.createScalarArray(ScalarType.pvString),
	                    			valueStructure 
	                    			} 
	                    		);
	            
	            PVStructure result = PVDataFactory.getPVDataCreate().createPVStructure(resultStructure);

	            PVStringArray labelsArray = (PVStringArray) result.getScalarArrayField("labels",ScalarType.pvString);
	            labelsArray.put(0, columnNames.length, columnNames, 0);

	            PVStructure valuesStructure = result.getStructureField("value");
	            PVDoubleArray epochSecondsArray = (PVDoubleArray) valuesStructure.getScalarArrayField("epochSeconds",ScalarType.pvDouble);
	            PVDoubleArray valuesArray = (PVDoubleArray) valuesStructure.getScalarArrayField("values",ScalarType.pvDouble);
	            PVDoubleArray nanosArray = (PVDoubleArray) valuesStructure.getScalarArrayField("nanos",ScalarType.pvDouble);
	            PVIntArray severityArray = (PVIntArray) valuesStructure.getScalarArrayField("severity",ScalarType.pvInt);
	            PVIntArray statusArray = (PVIntArray) valuesStructure.getScalarArrayField("status",ScalarType.pvInt);

	            // Call the server and get the data
				long before = System.currentTimeMillis();
				RawDataRetrieval rawDataRetrieval = new RawDataRetrieval(serverURL);
				HashMap<String, String> extraParams = new HashMap<String,String>();
				GenMsgIterator strm = rawDataRetrieval.getDataForPV(pvName, start, end, false, extraParams);

				if(strm != null) { 
					strm.onInfoChange(this);
				}

				try {
					List<Double> timeStamps = new LinkedList<Double>();
					List<Double> values = new LinkedList<Double>();
					List<Double> nanos = new LinkedList<Double>();
					List<Integer> severities = new LinkedList<Integer>();
					List<Integer> statuses = new LinkedList<Integer>();
					for(EpicsMessage dbrevent : strm) {
						timeStamps.add(new Double(dbrevent.getTimestamp().getTime()));
						values.add(dbrevent.getNumberValue().doubleValue());
						nanos.add(new Double(dbrevent.getTimestamp().getNanos()));
						severities.add(dbrevent.getSeverity());
						statuses.add(dbrevent.getStatus());
					}
					int totalValues = timeStamps.size();
					epochSecondsArray.put(0, totalValues, timeStamps.stream().mapToDouble(Double::doubleValue).toArray(), 0);
					valuesArray.put(0, totalValues, values.stream().mapToDouble(Double::doubleValue).toArray(), 0);
					nanosArray.put(0, totalValues, nanos.stream().mapToDouble(Double::doubleValue).toArray(), 0);
					severityArray.put(0, totalValues, severities.stream().mapToInt(Integer::intValue).toArray(), 0);
					statusArray.put(0, totalValues, statuses.stream().mapToInt(Integer::intValue).toArray(), 0);
					long after = System.currentTimeMillis();
					logger.info("Retrieved " + totalValues	+ " values  for pv " + pvName + " in " + (after-before) + "(ms)");
				} finally {
					if(strm != null) { strm.close(); }
				}
				
				return result;
			} catch(Throwable t) {
				logger.error("Exception fetching data for pv {}", pvName, t);
				throw new RPCRequestException(StatusType.ERROR, "Exception getting data from the server", t);
			}
		}

		@Override
		public void handleInfoChange(PayloadInfo info) {
		}
		
		private enum TimeParsing { 
			START,
			END
		}
		
		private Timestamp parseDateTime(String timeStr, TimeParsing startOrEnd) {
			try { 
				DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
				ZonedDateTime dt = ZonedDateTime.parse(timeStr, formatter);
				long epochSeconds = dt.getLong(ChronoField.INSTANT_SECONDS);
				logger.debug("Epochseconds for {} is {}", timeStr, epochSeconds);
				Timestamp ts = new Timestamp(epochSeconds*1000);
				return ts;
			} catch(DateTimeParseException ex) { 
				logger.error("Exception parsing {}", timeStr, ex);
				throw ex;
			}
		}
	}

	
}
