/**
 * helloWorld is meant to illustrate the simplest example of a remote procedure call (RPC)
 * style interaction between a client and a server using EPICS V4. 
 */
package org.epics.archiverappliance.v4service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.v4service.FetchDataFromAppliance;
import org.epics.pvaccess.PVAException;
import org.epics.pvaccess.server.rpc.RPCRequestException;
import org.epics.pvaccess.server.rpc.RPCServer;
import org.epics.pvaccess.server.rpc.RPCService;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status.StatusType;

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
            	// Bulk of the work done in FetchDataFromAppliance
            	FetchDataFromAppliance fetchData = new FetchDataFromAppliance(this.serverRetrievalURL, pvName, start, end);
            	PVStructure result = fetchData.getData();
            	return result;
            } catch(Exception ex) { 
            	logger.error("Exception fetching data from the appliance", ex);
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
	
}
