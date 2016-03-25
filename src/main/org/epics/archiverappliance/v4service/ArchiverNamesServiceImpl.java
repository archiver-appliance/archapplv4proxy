package org.epics.archiverappliance.v4service;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLEncoder;
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
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Names of PV's that are being archived etc
 * Support a pv=glob or regex=pattern arguments.
 * @author mshankar
 *
 */
class ArchiverNamesServiceImpl implements RPCService {
	private static final Logger logger = LogManager.getLogger();
	private String serverRetrievalURL;
	
	private static final StandardPVField standardFieldCreate = StandardPVFieldFactory.getStandardPVField();
	
	public ArchiverNamesServiceImpl(String serverRetrievalURL) { 
		this.serverRetrievalURL = serverRetrievalURL;
	}
	
	@SuppressWarnings("unchecked")
	public PVStructure request(PVStructure args) throws RPCRequestException {
		try { 
			PVStructure query = args.getStructureField("query");
			String pvName = query.getStringField("pv") != null ? query.getStringField("pv").get() : null;
			String regex = query.getStringField("regex") != null ? query.getStringField("regex").get() : null;
			StringWriter buf = new StringWriter();
			buf.append(serverRetrievalURL);
			if(pvName != null) { 
				buf.append("?pv=");
				buf.append(URLEncoder.encode(pvName, "UTF-8"));
			} else { 
				buf.append("?regex=");
				buf.append(URLEncoder.encode(regex, "UTF-8"));
			}
			PVStructure result = standardFieldCreate.scalarArray(ScalarType.pvString, "");
			JSONArray matchingPVs = getURLContentAsJSONArray(buf.toString());
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
	
	private static JSONArray getURLContentAsJSONArray(String urlStr) {
		try {
			logger.debug("Getting the contents of {} as a JSON array.", urlStr);
			URL retrievalMatchNamesURL = new URL(urlStr);
			JSONParser parser=new JSONParser();
			try (InputStream is = retrievalMatchNamesURL.openStream()) {
				return (JSONArray) parser.parse(new InputStreamReader(is));
			}
		} catch (IOException ex) {
			logger.error("Exception getting contents of internal URL " + urlStr, ex);
		} catch (ParseException pex) {
			logger.error("Parse exception getting contents of internal URL " + urlStr + " at " + pex.getPosition(), pex);
		}
		return null;
	}
}