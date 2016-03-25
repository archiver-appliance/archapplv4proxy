package org.epics.archiverappliance.v4service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.pvaccess.server.rpc.RPCRequestException;
import org.epics.pvaccess.server.rpc.RPCService;
import org.epics.pvdata.factory.StandardPVFieldFactory;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.StandardPVField;
import org.epics.pvdata.pv.Status.StatusType;
import org.epics.pvdata.pv.StringArrayData;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Takes in a list of PV's as a string array and then returns a string array of PV's that are being archived. 
 * This implements a filter compatible with the DS filter logic. 
 * @author mshankar
 *
 */
class ArchiverNamesFilterServiceImpl implements RPCService {
	private static final Logger logger = LogManager.getLogger();
	private String serverRetrievalURL;
	
	private static final StandardPVField standardFieldCreate = StandardPVFieldFactory.getStandardPVField();
	
	public ArchiverNamesFilterServiceImpl(String serverRetrievalURL) { 
		this.serverRetrievalURL = serverRetrievalURL;
	}
	
	@SuppressWarnings("unchecked")
	public PVStructure request(PVStructure args) throws RPCRequestException {
		try { 
			PVStringArray namesArray = (PVStringArray) args.getScalarArrayField("names", ScalarType.pvString);
			if(namesArray == null) { 
				throw new RPCRequestException(StatusType.ERROR, "For filters to work, create a structure with a names StringArrayField.");
			}
			
			PVStructure result = standardFieldCreate.scalarArray(ScalarType.pvString, "");
			JSONArray matchingPVs = getURLContentAsJSONArray(serverRetrievalURL, namesArray);
			if(matchingPVs != null && !matchingPVs.isEmpty()) {
				LinkedList<String> matchingPVsList = new LinkedList<String>();
				matchingPVsList.addAll(matchingPVs);
				PVStringArray machineNamesPVArray = (PVStringArray) result.getScalarArrayField("value", ScalarType.pvString);
				machineNamesPVArray.put(0, matchingPVsList.size(), matchingPVsList.toArray(new String[0]), 0);
			}
			return result;
		} catch(Exception ex) { 
			throw new RPCRequestException(StatusType.ERROR, ex.getMessage(), ex);
		}

	}
	
	@SuppressWarnings("unchecked")
	private static JSONArray getURLContentAsJSONArray(String urlStr, PVStringArray namesArray) throws IOException, ParseException {
		logger.debug("Getting the contents of {} as a JSON array.", urlStr);
		StringArrayData sar = new StringArrayData();
		namesArray.get(0, namesArray.getLength(), sar);
		JSONArray namesJSONObj = new JSONArray();
		for(String name : sar.data) { 
			if(name != null && !name.isEmpty()) { 
				logger.debug("Adding pv name {}",  name);
				namesJSONObj.add(name);
			}
		}
		
		if(namesJSONObj.isEmpty()) { 
			return new JSONArray();
		}
		
		String namesJSONStr = namesJSONObj.toJSONString();
		
		JSONParser parser=new JSONParser();
		URL retrievalMatchNamesURL = new URL(urlStr);
		HttpURLConnection conn = (HttpURLConnection) retrievalMatchNamesURL.openConnection();
		conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Content-Length", String.valueOf(namesJSONStr.length()));
		conn.setDoOutput(true);
		try {
			OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());
			writer.write(namesJSONStr);
			writer.flush();
			logger.debug("Made the call to the appliance " + retrievalMatchNamesURL);
			
			BufferedReader reader = new BufferedReader(new InputStreamReader((conn.getInputStream()), StandardCharsets.UTF_8));
	        JSONArray respJSONObj = (JSONArray) parser.parse(reader);
			logger.debug("Done parsing response from  " + retrievalMatchNamesURL);
	        
	        writer.close();
	        reader.close();
	        return respJSONObj;
		} finally { 
	        conn.disconnect();				
		}
	}
}