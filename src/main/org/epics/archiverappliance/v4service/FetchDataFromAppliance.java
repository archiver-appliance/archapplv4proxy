package org.epics.archiverappliance.v4service;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.retrieval.client.EpicsMessage;
import org.epics.archiverappliance.retrieval.client.GenMsgIterator;
import org.epics.archiverappliance.retrieval.client.InfoChangeHandler;
import org.epics.archiverappliance.retrieval.client.RawDataRetrieval;
import org.epics.pvaccess.server.rpc.RPCRequestException;
import org.epics.pvdata.factory.ConvertFactory;
import org.epics.pvdata.factory.FieldFactory;
import org.epics.pvdata.factory.PVDataFactory;
import org.epics.pvdata.pv.Convert;
import org.epics.pvdata.pv.Field;
import org.epics.pvdata.pv.FieldCreate;
import org.epics.pvdata.pv.PVByte;
import org.epics.pvdata.pv.PVByteArray;
import org.epics.pvdata.pv.PVDataCreate;
import org.epics.pvdata.pv.PVDouble;
import org.epics.pvdata.pv.PVDoubleArray;
import org.epics.pvdata.pv.PVFloat;
import org.epics.pvdata.pv.PVFloatArray;
import org.epics.pvdata.pv.PVInt;
import org.epics.pvdata.pv.PVIntArray;
import org.epics.pvdata.pv.PVLongArray;
import org.epics.pvdata.pv.PVScalar;
import org.epics.pvdata.pv.PVScalarArray;
import org.epics.pvdata.pv.PVShort;
import org.epics.pvdata.pv.PVShortArray;
import org.epics.pvdata.pv.PVString;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.PVUnion;
import org.epics.pvdata.pv.PVUnionArray;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.Status.StatusType;
import org.epics.pvdata.pv.Structure;
import org.epics.pvdata.pv.Union;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadType;

/**
 * Make a call to the specified appliance; get the data using the raw protocol and then convert it to a NTTable.
 * We have two helped interfaces; one to handle the various forms of specifying time and the other to handle the various DBR types.
 * @author mshankar
 *
 */
public class FetchDataFromAppliance implements InfoChangeHandler  {
	private static Logger logger = LogManager.getLogger();
	
	String pvName;
	Timestamp start;
	Timestamp end;
	String serverURL;
	
	private static final FieldCreate fieldCreate = FieldFactory.getFieldCreate();
	private static final PVDataCreate pvDataCreate = PVDataFactory.getPVDataCreate();
	private static final Convert pvDataConvert = ConvertFactory.getConvert();

	
	public FetchDataFromAppliance(String serverDataRetrievalURL, String pvName, String startStr, String endStr) throws ParseException {
		this.serverURL = serverDataRetrievalURL;
		this.pvName = pvName;
		StartEndTime stendTime = StartEndTime.parse(startStr, endStr);
		this.start = stendTime.getStartTimestamp();
		this.end = stendTime.getEndTimestamp();
	}

	public PVStructure getData() throws Exception {
		try {
			logger.debug("Getting data for pv {} from {} to {}", pvName, start.getTime(), end.getTime());
			long before = System.currentTimeMillis();

			// Call the server and get the data using pbraw/HTTP
			RawDataRetrieval rawDataRetrieval = new RawDataRetrieval(serverURL);
			HashMap<String, String> extraParams = new HashMap<String,String>();
			GenMsgIterator strm = rawDataRetrieval.getDataForPV(pvName, start, end, false, extraParams);
			if(strm == null) { 
				return null;
			}
			
			// Register for info changes; we may have to do something with this later.
			strm.onInfoChange(this);
			
			// Determine the type of the .VAL
			ValueHandler valueHandler = determineValueHandler(strm.getPayLoadInfo());

			try {
				List<Long> timeStamps = new LinkedList<Long>();
				List<Integer> nanos = new LinkedList<Integer>();
				List<Integer> severities = new LinkedList<Integer>();
				List<Integer> statuses = new LinkedList<Integer>();
				for(EpicsMessage dbrevent : strm) {
					timeStamps.add(new Long(dbrevent.getTimestamp().getTime()/1000));
					// The valueHandler knows what to do with the various DBR types.
					valueHandler.handleMessage(dbrevent);
					nanos.add(dbrevent.getTimestamp().getNanos());
					severities.add(dbrevent.getSeverity());
					statuses.add(dbrevent.getStatus());
				}

				int totalValues = timeStamps.size();
				PVStructure result = createResultStructure(strm.getPayLoadInfo(), valueHandler);
				logger.debug(result.getStructure().toString());
				
				valueHandler.addToResult(result, totalValues);
				
				PVStructure structureContainingTimeFields = result.getStructureField("value");

				PVLongArray epochSecondsArray = (PVLongArray) structureContainingTimeFields.getScalarArrayField("secondsPastEpoch",ScalarType.pvLong);
				epochSecondsArray.put(0, totalValues, timeStamps.stream().mapToLong(Long::longValue).toArray(), 0);

				PVIntArray nanosArray = (PVIntArray) structureContainingTimeFields.getScalarArrayField("nanoseconds",ScalarType.pvInt);
				nanosArray.put(0, totalValues, nanos.stream().mapToInt(Integer::intValue).toArray(), 0);

				PVIntArray severityArray = (PVIntArray) structureContainingTimeFields.getScalarArrayField("severity",ScalarType.pvInt);
				severityArray.put(0, totalValues, severities.stream().mapToInt(Integer::intValue).toArray(), 0);

				PVIntArray statusArray = (PVIntArray) structureContainingTimeFields.getScalarArrayField("status",ScalarType.pvInt);
				statusArray.put(0, totalValues, statuses.stream().mapToInt(Integer::intValue).toArray(), 0);
				
				long after = System.currentTimeMillis();
				logger.info("Retrieved " + totalValues	+ " values  for pv " + pvName + " in " + (after-before) + "(ms)");
				return result;
			} finally {
				strm.close();
			}
			
		} catch(Throwable t) {
			logger.error("Exception fetching data for pv {}", pvName, t);
			throw new RPCRequestException(StatusType.ERROR, "Exception getting data from the server", t);
		}
	}
	@Override
	public void handleInfoChange(PayloadInfo info) {
	}
	
	/**
	 * Both scalars and vectors are packed into a NTComplexTable
	 *  NTComplexTable := 
	 *  structure
	 *  	string[]   labels              // Very short text describing each field below, i.e. column labels
	 *  structure  value
	 *    {union[] colname }0+ // 0 or more union arrays, each of which can be one of the following types.
	 *      union
	 *  	  NTScalar scalarValue   // Use for DBR_SCALAR…
	 *  	  NTScalarArray scalarArrayValue // Use for DBR_WAVEFORM…
	 *  	  NTNDArray ntndArrayValue // Use for areaDetector…
	 *  	  NTComplexTable complextTableValue // Nested Complex Table for NoSQL types
	 *  string     descriptor  : opt
	 *  alarm_t    alarm       : opt
	 *  time_t     timeStamp   : opt
	 *
 	 * @param payloadInfo
	 * @param valueHandler
	 * @return
	 */
	private PVStructure createResultStructure(PayloadInfo payloadInfo, ValueHandler valueHandler) throws RPCRequestException{
		Union valueUnion = null;
		if(scalarTypes.contains(payloadInfo.getType())) { 
			valueUnion = fieldCreate.createUnion(
					new String[] {"scalarValue"}, 
					new Field[] { fieldCreate.createScalar(valueHandler.getValueType())});
		} else if(waveformTypes.contains(payloadInfo.getType())) { 
			valueUnion = fieldCreate.createUnion(
					new String[] {"scalarArrayValue"}, 
					new Field[] { fieldCreate.createScalarArray(valueHandler.getValueType())});
		} else { 
			String msg = "Cannot determine union type for " + payloadInfo.getType();
			logger.error(msg);
			throw new RPCRequestException(StatusType.ERROR, msg);
		}
	
		
		// Create the result structure of the data interface.
		String[] columnNames = new String[]{"secondsPastEpoch", "values", "nanoseconds", "severity", "status"};

		Structure valueStructure = fieldCreate.createStructure(
				columnNames,
				 new Field[] {
		                fieldCreate.createScalarArray(ScalarType.pvLong),
		                fieldCreate.createUnionArray(valueUnion),
		                fieldCreate.createScalarArray(ScalarType.pvInt),
		                fieldCreate.createScalarArray(ScalarType.pvInt),
		                fieldCreate.createScalarArray(ScalarType.pvInt),
		                }
				);
		Structure resultStructure =
		        fieldCreate.createStructure( "epics:nt/NTComplexTable:1.0",
		                new String[] { "labels", "value" },
		                new Field[] { 
		        			fieldCreate.createScalarArray(ScalarType.pvString),
		        			valueStructure 
		        			} 
		        		);
		
		PVStructure result = pvDataCreate.createPVStructure(resultStructure);

		PVStringArray labelsArray = (PVStringArray) result.getScalarArrayField("labels",ScalarType.pvString);
		labelsArray.put(0, columnNames.length, columnNames, 0);
		return result;
	}

	/**
	 * Interface to help with the various DBR Types.
	 */
	private interface ValueHandler {
		/**
		 * Add the value from the event into whatever state is being used to collect the values
		 * @param dbrevent
		 */
		public void handleMessage(EpicsMessage dbrevent) throws IOException;
		/**
		 * What PVData scalar type should we use when creating the field in PVStructure
		 * @return
		 */
		public ScalarType getValueType();
		/**
		 * We are done; now stuff all the values into the result;
		 * @param structure -  The result structure....
		 * @param totalValues - The number of elements to stuff into the structure
		 */
		public void addToResult(PVStructure result, int totalValues);		
	}

	
	/**
	 * @author mshankar
	 * Helper generic to handle the bulk of the DBR type boilerplate
	 *
	 * @param <JavaType> - This is the Java object type for the primitive - for example, Double for double
	 * @param <PVDataType> - This is the PVData object type for the array result - for example PVDoubleArray for DBR_DOUBLE.
	 */
	private class ScalarValueHandler<JavaType, PVDataType extends PVScalar> implements ValueHandler {
		ScalarType valueType;
		BiConsumer<EpicsMessage, List<JavaType>> handleMessageFunction;
		BiConsumer<JavaType, PVDataType> putIntoValuesFunction;
		LinkedList<JavaType> values = new LinkedList<JavaType>();

		public ScalarValueHandler(ScalarType valueType, BiConsumer<EpicsMessage, List<JavaType>> handleMessageFunction, BiConsumer<JavaType, PVDataType> putIntoValuesFunction) {
			this.valueType = valueType;
			this.handleMessageFunction = handleMessageFunction;
			this.putIntoValuesFunction = putIntoValuesFunction;
		}

		@Override
		public ScalarType getValueType() {
			return valueType;
		}

		@Override
		public void handleMessage(EpicsMessage dbrevent) throws IOException {
			handleMessageFunction.accept(dbrevent,  values);
		}

		@Override
		public void addToResult(PVStructure result, int totalValues) { 
			PVStructure valuesStructure = result.getStructureField("value");
			PVUnionArray valuesArray = (PVUnionArray) valuesStructure.getUnionArrayField("values");
			PVUnion[] unionArray = new PVUnion[totalValues];
			for(int i = 0; i < totalValues; i++) {  
				unionArray[i] = pvDataCreate.createPVUnion(valuesArray.getUnionArray().getUnion());
				@SuppressWarnings("unchecked")
				PVDataType valueScalar = (PVDataType) pvDataCreate.createPVScalar(valueType);
				putIntoValuesFunction.accept(values.get(i), valueScalar);
				unionArray[i].set("scalarValue", valueScalar);
			}
			valuesArray.put(0, unionArray.length, unionArray, 0);
		}		
	}

	/**
	 * @author mshankar
	 * Helper generic to handle the bulk of the DBR type boilerplate for waveforms of numbers
	 *
	 * @param <JavaType> - This is the Java object type for the primitive - for example, Double for double
	 * @param <PVDataType> - This is the PVData object type for the array result - for example PVDoubleArray for DBR_DOUBLE.
	 */
	private class WaveformValueHandler<JavaType, PVDataType extends PVScalar, PVWaveformDataType extends PVScalarArray> implements ValueHandler {
		ScalarType valueType;
		BiFunction<EpicsMessage, Integer, JavaType> getSampleValueAtFunction;
		BiConsumer<PVWaveformDataType, List<JavaType>> putSamplesIntoStructureFunction;
		LinkedList<List<JavaType>> values = new LinkedList<List<JavaType>>();

		public WaveformValueHandler(ScalarType valueType, BiFunction<EpicsMessage, Integer, JavaType> getSampleValueAtFunction, BiConsumer<PVWaveformDataType, List<JavaType>> putSamplesIntoStructureFunction) {
			this.valueType = valueType;
			this.getSampleValueAtFunction = getSampleValueAtFunction;
			this.putSamplesIntoStructureFunction = putSamplesIntoStructureFunction;
		}

		@Override
		public ScalarType getValueType() {
			return valueType;
		}

		@Override
		// handleMessage is handled in the final implementation; the thrown IOException makes it difficult to use a lambda
		public void handleMessage(EpicsMessage dbrevent) throws IOException { 
			int totalEvents = dbrevent.getElementCount();
			logger.debug("Adding {} events ", totalEvents);
			ArrayList<JavaType> sampleValues = new ArrayList<JavaType>();
			for(int i = 0; i < totalEvents; i++) {
				sampleValues.add(getSampleValueAtFunction.apply(dbrevent, i));
			}
			values.add(sampleValues);
		}
		
		@Override
		public void addToResult(PVStructure result, int totalValues) {
			PVStructure valuesStructure = result.getStructureField("value");
			PVUnionArray valuesArray = (PVUnionArray) valuesStructure.getUnionArrayField("values");
			PVUnion[] unionArray = new PVUnion[totalValues];
			for(int i = 0; i < totalValues; i++) {  
				unionArray[i] = pvDataCreate.createPVUnion(valuesArray.getUnionArray().getUnion());
				@SuppressWarnings("unchecked")
				PVWaveformDataType valueScalar = (PVWaveformDataType) pvDataCreate.createPVScalarArray(valueType);
				putSamplesIntoStructureFunction.accept(valueScalar, values.get(i));
				unionArray[i].set("scalarArrayValue", valueScalar);
			}
			valuesArray.put(0, unionArray.length, unionArray, 0);
		}
	}

	/**
	 * Use the type information in the payloadInfo to construct a ValueHandler that can convert the DBR_TYPE into the corresponding PVScalar array
	 * @param payloadInfo
	 * @return
	 * @throws IOException
	 */
	private ValueHandler determineValueHandler(PayloadInfo payloadInfo) throws IOException { 
		// Bulk of this is boilerplate. Use the SCALAR_DOUBLE and WAVEFORM_DOUBLE as an template.
		switch(payloadInfo.getType()) {
		case SCALAR_DOUBLE:
			return new ScalarValueHandler<Double, PVDouble>(
					ScalarType.pvDouble, 
					(dbrevent, values) -> values.add(dbrevent.getNumberValue().doubleValue()),
					(value, valueScalar) -> valueScalar.put(value.doubleValue()));
		case SCALAR_BYTE:
			return new ScalarValueHandler<Byte,PVByte>(
					ScalarType.pvByte, 
					(dbrevent, values) -> values.add(dbrevent.getNumberValue().byteValue()),
					(value, valueScalar) -> valueScalar.put(value.byteValue()));
		case SCALAR_ENUM:
			return new ScalarValueHandler<Integer,PVInt>(
					ScalarType.pvInt, 
					(dbrevent, values) -> values.add(dbrevent.getNumberValue().intValue()),
					(value, valueScalar) -> valueScalar.put(value.intValue()));
		case SCALAR_FLOAT:
			return new ScalarValueHandler<Float,PVFloat>(
					ScalarType.pvFloat, 
					(dbrevent, values) -> values.add(dbrevent.getNumberValue().floatValue()),
					(value, valueScalar) -> valueScalar.put(value.floatValue()));
		case SCALAR_INT:
			return new ScalarValueHandler<Integer,PVInt>(
					ScalarType.pvInt, 
					(dbrevent, values) -> values.add(dbrevent.getNumberValue().intValue()),
					(value, valueScalar) -> valueScalar.put(value.intValue()));
		case SCALAR_SHORT:
			return new ScalarValueHandler<Short,PVShort>(
					ScalarType.pvShort,
					(dbrevent, values) -> values.add(dbrevent.getNumberValue().shortValue()),
					(value, valueScalar) -> valueScalar.put(value.shortValue()));
		case SCALAR_STRING:
			return new ScalarValueHandler<String,PVString>(
					ScalarType.pvString,
					(dbrevent, values) -> values.add(dbrevent.getStringValue()),
					(value, valueScalar) -> valueScalar.put(value));
		case V4_GENERIC_BYTES:
			throw new UnsupportedOperationException();
		case WAVEFORM_BYTE:
			return new WaveformValueHandler<Byte,PVByte, PVByteArray>(
					ScalarType.pvByte,
					(dbrevent, index) -> dbrevent.getNumberAt(index).byteValue(),
					(sampleValues, srcValues) -> sampleValues.put(0, srcValues.size(), Bytes.toArray(srcValues), 0));
		case WAVEFORM_DOUBLE:
			return new WaveformValueHandler<Double,PVDouble, PVDoubleArray>(
					ScalarType.pvDouble,
					(dbrevent, index) -> dbrevent.getNumberAt(index).doubleValue(),
					(sampleValues, srcValues) -> sampleValues.put(0, srcValues.size(), Doubles.toArray(srcValues), 0));
		case WAVEFORM_ENUM:
			return new WaveformValueHandler<Integer,PVInt, PVIntArray>(
					ScalarType.pvInt,
					(dbrevent, index) -> dbrevent.getNumberAt(index).intValue(),
					(sampleValues, srcValues) -> sampleValues.put(0, srcValues.size(), Ints.toArray(srcValues), 0));
		case WAVEFORM_FLOAT:
			return new WaveformValueHandler<Float,PVFloat, PVFloatArray>(
					ScalarType.pvFloat,
					(dbrevent, index) -> dbrevent.getNumberAt(index).floatValue(),
					(sampleValues, srcValues) -> sampleValues.put(0, srcValues.size(), Floats.toArray(srcValues), 0));
		case WAVEFORM_INT:
			return new WaveformValueHandler<Integer,PVInt, PVIntArray>(
					ScalarType.pvInt,
					(dbrevent, index) -> dbrevent.getNumberAt(index).intValue(),
					(sampleValues, srcValues) -> sampleValues.put(0, srcValues.size(), Ints.toArray(srcValues), 0));
		case WAVEFORM_SHORT:
			return new WaveformValueHandler<Short,PVShort, PVShortArray>(
					ScalarType.pvShort,
					(dbrevent, index) -> dbrevent.getNumberAt(index).shortValue(),
					(sampleValues, srcValues) -> sampleValues.put(0, srcValues.size(), Shorts.toArray(srcValues), 0));
		case WAVEFORM_STRING:
			throw new UnsupportedOperationException();
		default:
			break; 
		}
		
		return null;
	}
	
	
	private static List<PayloadType> scalarTypes = Arrays.asList(PayloadType.SCALAR_BYTE, 
			PayloadType.SCALAR_DOUBLE, 
			PayloadType.SCALAR_ENUM,
			PayloadType.SCALAR_FLOAT,
			PayloadType.SCALAR_INT,
			PayloadType.SCALAR_SHORT,
			PayloadType.SCALAR_STRING);
	
	private static List<PayloadType> waveformTypes = Arrays.asList(PayloadType.WAVEFORM_BYTE, 
			PayloadType.WAVEFORM_DOUBLE, 
			PayloadType.WAVEFORM_ENUM,
			PayloadType.WAVEFORM_FLOAT,
			PayloadType.WAVEFORM_INT,
			PayloadType.WAVEFORM_SHORT,
			PayloadType.WAVEFORM_STRING);
	
	/**
	 * Return multiple PVStructure's in some kind of object for use when the request has multiple PV's
	 * @return
	 */
	public static PVStructure createMultiplePVResultStructure(LinkedHashMap<String, PVStructure> results) { 
		Structure finalResultStructure = fieldCreate.createStructure(
				new String[] { "data"},
                new Field[] { fieldCreate.createVariantUnionArray() });
		
		LinkedList<PVUnion> resultUnions = new LinkedList<PVUnion>();
		
		for(String pvName : results.keySet()) { 
			PVStructure pvStruct = results.get(pvName);
			Structure eachPVStruct = fieldCreate.createStructure(
					new String[] { "pvName", "data"},
	                new Field[] { fieldCreate.createScalar(ScalarType.pvString), fieldCreate.createStructure(pvStruct.getStructure()) });
			PVStructure eachPVData = pvDataCreate.createPVStructure(eachPVStruct);
			eachPVData.getStringField("pvName").put(pvName);
			pvDataConvert.copyStructure(pvStruct, eachPVData.getStructureField("data"));
			PVUnion resultUnion = pvDataCreate.createPVVariantUnion();
			resultUnion.set(eachPVData);
			resultUnions.add(resultUnion);
		}
		
		PVStructure result = pvDataCreate.createPVStructure(finalResultStructure);
		result.getUnionArrayField("data").put(0, resultUnions.size(), resultUnions.toArray(new PVUnion[0]), 0);
		
		return result;
	}
}


