package org.epics.archiverappliance.v4service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.epics.pvaccess.ClientFactory;
import org.epics.pvaccess.client.rpc.RPCClient;
import org.epics.pvaccess.client.rpc.RPCClientFactory;
import org.epics.pvaccess.client.rpc.RPCClientRequester;
import org.epics.pvdata.factory.FieldFactory;
import org.epics.pvdata.factory.PVDataFactory;
import org.epics.pvdata.pv.FieldBuilder;
import org.epics.pvdata.pv.FieldCreate;
import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.PVDataCreate;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.StringArrayData;

/**
 * Sample to exercise the filter functionality.
 * @author mshankar
 *
 */
public class FilterClientAsync implements RPCClientRequester {
	private static final FieldCreate fieldCreate = FieldFactory.getFieldCreate();
	private static final PVDataCreate pvDataCreate = PVDataFactory.getPVDataCreate();
	
	private String fileWithPVNames;
	private CountDownLatch latch = new CountDownLatch(1);


	public static void main(String[] args) throws Exception {
		FilterClientAsync theRequester = new FilterClientAsync();
		theRequester.fileWithPVNames = args[1];
		RPCClient client = RPCClientFactory.create(args[0] + ":filter", theRequester);
		try {
			System.out.println("Send the request across on thread " + Thread.currentThread().getName());
			theRequester.latch.await(20, TimeUnit.SECONDS);
		} finally { 
			client.destroy();
			ClientFactory.stop();
		}
	}


	@Override
	public String getRequesterName() {
		return "FilterClientAsync";
	}


	@Override
	public void message(String message, MessageType arg1) {
		System.err.println(message);
	}


	@Override
	public void connectResult(RPCClient client, Status status) {
		if(status.isOK()) {
			try { 
				String[] pvNames = Files.readAllLines(Paths.get(fileWithPVNames)).toArray(new String[0]);
				FieldBuilder fieldBuilder = fieldCreate.createFieldBuilder();
				fieldBuilder.addArray("names", ScalarType.pvString);
				PVStructure archivedPVs = pvDataCreate.createPVStructure(fieldBuilder.createStructure());
				PVStringArray stringArray = (PVStringArray) archivedPVs.getScalarArrayField("names", ScalarType.pvString);
				stringArray.put(0, pvNames.length, pvNames, 0);
				client.sendRequest(archivedPVs);
			} catch(IOException ex) { 
				ex.printStackTrace();
			}
		}
	}


	@Override
	public void requestResult(RPCClient client, Status status, PVStructure result) {
		System.out.println("Got the response on thread " + Thread.currentThread().getName());
		PVStringArray resp = (PVStringArray) result.getScalarArrayField("value", ScalarType.pvString);
		StringArrayData sar = new StringArrayData();
		resp.get(0, resp.getLength(), sar);
		for(int i = 0; i < sar.data.length; i++) { 
			System.out.println(sar.data[i]);
		}
		latch.countDown();
	}
}
