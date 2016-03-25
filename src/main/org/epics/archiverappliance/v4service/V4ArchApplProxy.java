/**
 * helloWorld is meant to illustrate the simplest example of a remote procedure call (RPC)
 * style interaction between a client and a server using EPICS V4. 
 */
package org.epics.archiverappliance.v4service;

import java.util.LinkedHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.pvaccess.PVAException;
import org.epics.pvaccess.server.rpc.RPCRequestException;
import org.epics.pvaccess.server.rpc.RPCServer;
import org.epics.pvaccess.server.rpc.RPCService;
import org.epics.pvdata.pv.PVString;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status.StatusType;

/**
 * V4ArchApplProxy is a proxy to the archiver appliance
 * 
 * @author Murali Shankar (mshankar@slac.stanford.edu)
 */
public class V4ArchApplProxy
{
	private static final Logger logger = LogManager.getLogger();
	private RPCServer server;

	/**
	 * Implementation of RPC service.
	 */
	static class ArchiverServiceImpl implements RPCService
	{
		private String serverRetrievalURL;
		private String serviceName;
		
		public ArchiverServiceImpl(String serviceName, String serverRetrievalURL) {
			this.serviceName = serviceName;
			this.serverRetrievalURL = serverRetrievalURL;
		}
		
		public PVStructure request(PVStructure args) throws RPCRequestException {
			PVStructure query = args.getStructureField("query");
			
			PVString userWantsHelp = query.getStringField("help");
			if(userWantsHelp != null) {
				PVString showPythonSample = query.getStringField("showpythonsample");
				return FetchDataFromAppliance.getHelpMessage(serviceName, showPythonSample != null);
			}
			

			
			String pvName = query.getStringField("pv").get();
			String start = query.getStringField("from") != null ? query.getStringField("from").get() : null;
			String end = query.getStringField("to") != null ? query.getStringField("to").get() : null;
			

			if(pvName != null) { 
				if(pvName.contains(",")) {
					// Multiple PV's, return as a PV structure array
		            try { 
		            	LinkedHashMap<String, PVStructure> resultStructures = new LinkedHashMap<String, PVStructure>();
		            	String[] pvNames = pvName.split(",");
		            	for(String multPVName : pvNames) { 
			            	// Bulk of the work done in FetchDataFromAppliance
			    			logger.debug("Getting data for pv {} from {} to {}", multPVName, start, end);
			            	FetchDataFromAppliance fetchData = new FetchDataFromAppliance(this.serverRetrievalURL, multPVName, start, end);
			            	PVStructure result = fetchData.getData();
			            	if(result != null) { 
			            		resultStructures.put(multPVName, result);
			            	}
		            	}
		            	if(resultStructures.isEmpty()) { 
		            		logger.error("No data found for any of the PV's in the request {}", String.join(",", pvNames));
		    				throw new RPCRequestException(StatusType.ERROR, "The PV's " + String.join(",", pvNames) + " are not being archived.");
		            	} else { 
			            	return FetchDataFromAppliance.createMultiplePVResultStructure(resultStructures);
		            	}
		            } catch(Exception ex) { 
		            	logger.error("Exception fetching data from the appliance", ex);
		            	throw new RPCRequestException(StatusType.ERROR, "Exception fetching data", ex);
		            }
				} else { 
		            try { 
		            	// Bulk of the work done in FetchDataFromAppliance
		    			logger.debug("Getting data for pv {} from {} to {}", pvName, start, end);
		            	FetchDataFromAppliance fetchData = new FetchDataFromAppliance(this.serverRetrievalURL, pvName, start, end);
		            	PVStructure result = fetchData.getData();
		            	if(result == null) { 
		            		logger.error("No data found for PV {}", String.join(",", pvName));
		    				throw new RPCRequestException(StatusType.ERROR, "The PV " + pvName + " is not being archived.");
		            	}
		            	return result;
		            } catch(Exception ex) { 
		            	logger.error("Exception fetching data from the appliance", ex);
		            	throw new RPCRequestException(StatusType.ERROR, "Exception fetching data", ex);
		            }
				}
			} else { 
				throw new RPCRequestException(StatusType.ERROR, "Cannot determine PV name for getting history");
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
			System.err.println("For example: java org.epics.archiverappliance.v4service.V4ArchApplProxy archProxy http://archiver.facility.org/retrieval/");
			return;
		}
		
		V4ArchApplProxy proxy = new V4ArchApplProxy();
		proxy.init(args);
		
	}
	
	

    /**
     * Method for jsvc - Apache Commons Daemon
     * Here open configuration files, create a trace file, create ServerSockets, Threads
     * @param arguments
     */
    public void init(String[] arguments) throws PVAException { 
		String serviceName = arguments[0];
		String serverRetrievalURL = arguments[1];
		if(!serverRetrievalURL.endsWith("/")) { 
			serverRetrievalURL = serverRetrievalURL + "/";
		}
		
		server = new RPCServer();

		// Register the service
		server.registerService(serviceName, new ArchiverServiceImpl(serviceName, serverRetrievalURL + "data/getData.raw"));
		server.registerService(serviceName+":search", new ArchiverNamesServiceImpl(serverRetrievalURL + "bpl/getMatchingPVs"));
		server.registerService(serviceName+":filter", new ArchiverNamesFilterServiceImpl(serverRetrievalURL + "bpl/archivedPVs"));
		logger.info("Starting the EPICS archiver appliance proxy under the service name {} proxying the server {}", serviceName, serverRetrievalURL);
		server.printInfo();
    }
    
    /**
     * Method for jsvc - Apache Commons Daemon
     * Start the Thread, accept incoming connections
     * @param arguments
     */
    public void start() throws PVAException { 
		Thread launcherThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try { 
					server.run(0);
				} catch(PVAException ex) { 
					logger.error("Exception starting service", ex);
				}
			}
		});
		launcherThread.setName("Launcher_Thread");
		launcherThread.start();
    }
    
    /**
     * Method for jsvc - Apache Commons Daemon
     * Inform the Thread to terminate the run(), close the ServerSockets
     * @param arguments
     */
    public void stop() throws PVAException { 
    	server.destroy(); 
    	server = null;

    }
    
    /**
     * Method for jsvc - Apache Commons Daemon
     * Destroy any object created in init()
     * @param arguments
     */
    public void destroy() { 
    }
}
