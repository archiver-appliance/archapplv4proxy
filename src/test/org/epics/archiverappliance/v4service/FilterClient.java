package org.epics.archiverappliance.v4service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.epics.pvaccess.ClientFactory;
import org.epics.pvaccess.client.rpc.RPCClient;
import org.epics.pvaccess.client.rpc.RPCClientFactory;
import org.epics.pvaccess.server.rpc.RPCRequestException;
import org.epics.pvdata.factory.FieldFactory;
import org.epics.pvdata.factory.PVDataFactory;
import org.epics.pvdata.pv.FieldBuilder;
import org.epics.pvdata.pv.FieldCreate;
import org.epics.pvdata.pv.PVDataCreate;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.StringArrayData;

/**
 * Sample to exercise the filter functionality.
 * @author mshankar
 *
 */
public class FilterClient {
	private static final FieldCreate fieldCreate = FieldFactory.getFieldCreate();
	private static final PVDataCreate pvDataCreate = PVDataFactory.getPVDataCreate();


	public static void main(String[] args) throws IOException, RPCRequestException {
		RPCClient client = RPCClientFactory.create(args[0] + ":filter");
		try { 
			String[] pvNames = Files.readAllLines(Paths.get(args[1])).toArray(new String[0]);
			FieldBuilder fieldBuilder = fieldCreate.createFieldBuilder();
			fieldBuilder.addArray("names", ScalarType.pvString);
			PVStructure archivedPVs = pvDataCreate.createPVStructure(fieldBuilder.createStructure());
			PVStringArray stringArray = (PVStringArray) archivedPVs.getScalarArrayField("names", ScalarType.pvString);
			stringArray.put(0, pvNames.length, pvNames, 0);
			PVStringArray resp = (PVStringArray) client.request(archivedPVs, 10).getScalarArrayField("value", ScalarType.pvString);
			StringArrayData sar = new StringArrayData();
			resp.get(0, resp.getLength(), sar);
			for(int i = 0; i < sar.data.length; i++) { 
				System.out.println(sar.data[i]);
			}
			
		} finally { 
			client.destroy();
			ClientFactory.stop();
		}
	}

}
