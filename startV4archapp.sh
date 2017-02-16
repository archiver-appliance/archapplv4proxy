#!/bin/bash

# Sample startup script for starting the EPICS Archiver Appliance V4 proxy.
# Since this is to be shared publicly, please do nor hardcode URL's and paths in this script.
# Have another type II script that calls this one with the appropriate arguments and environment variables.

# Arguments
# 1: ServiceName - This is the name of the service.
# 2: ApplianceURL - This is the data_retrieval_url from your appliances.xml and looks something like so http://applicance.lab.edu:17668/retrieval
# 3: start|stop|run - Start or stop the service. Run starts the service in the foreground using java (that is, run does not use jsvc).

SERVICE_NAME=$1
APPLIANCE_URL=$2
COMMAND=$3

# Environment Variables
# 1: LOGS_FOLDER - This is the folder where the logs are written to. This is also the folder containing the PID file.
# 2: JAVA_HOME - Use a 1.8 JDK.
# 3: COMMONS_DAEMON_FOLDER - Folder containing the Apache Commons Daemon folder for this OS. 
# 4: EPICS V4 specific environment variables.

if [ $# -ne 3 ]; then
    echo "Usage: $0 <ServiceName> <Appliance_URL> start|stop|run"
    exit 1
fi

# Startup script for the V4 archiver service. Bulk of the configuration is done here.
pushd `dirname $0`
SCRIPT_DIR=`pwd`
popd
echo "Running the EPICS Archiver Appliance V4 proxy from ${SCRIPT_DIR}"

if [[ -z "$LOGS_FOLDER" ]]
then
  LOGS_FOLDER=.
  if [[ ! -z ${PHYSICS_DATA} ]]
  then
    LOGS_FOLDER=${PHYSICS_DATA}/log
  fi
fi


[ -z "$PACKAGE_TOP" ] && export PACKAGE_TOP="/afs/slac/g/lcls/package"

if [[ `arch` == "x86_64" ]]
then
  ARCH="linux-x86_64"
else
  ARCH="linux-x86"
fi

export JAVA_HOME=/afs/slac/g/lcls/package/java/jdk1.8.0_60/${ARCH}
export PATH=${JAVA_HOME}/bin:${PATH}

echo "Using java in folder ${JAVA_HOME}"
java -version


[ -z "$COMMONS_DAEMON_FOLDER" ] && COMMONS_DAEMON_FOLDER=${PACKAGE_TOP}/commons-daemon/1.0.15/${ARCH}
JSVC_BIN=${COMMONS_DAEMON_FOLDER}/jsvc
echo "Using jsvc (Apache Commons Daemon) located at ${JSVC_BIN}"

[ -z "$LOGS_FOLDER" ] && echo "Please set the LOGS_FOLDER environment variable" && exit 1;
[ -z "$JAVA_HOME" ] && echo "Please set the JAVA_HOME environment variable" && exit 1;
if [ ${COMMAND} != "run" ]; then
  [ -z "$COMMONS_DAEMON_FOLDER" ] && echo "Please set the COMMONS_DAEMON_FOLDER environment variable" && exit 1;
fi



HOSTNAME=`hostname`

if [[ -z "$LOG_CONFIG_FILE" ]]
then
	echo "Logs are being generated in the folder ${LOGS_FOLDER}"
	export LOG_CONFIG_FILE=${LOGS_FOLDER}/${HOSTNAME}_archappl_logconfig.xml
	
	# Generate the default log4j config file. This is production level logging by default.
	echo "Generating default log configuration in ${LOGS_FOLDER}/${HOSTNAME}_archappl_logconfig.xml"
	cat > ${LOGS_FOLDER}/${HOSTNAME}_archappl_logconfig.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="ERROR">
 <Appenders>
  <RollingFile name="RollingFile" fileName="${LOGS_FOLDER}/${HOSTNAME}_archappl_v4proxy.log" filePattern="${LOGS_FOLDER}/${HOSTNAME}_archappl_v4proxy-%d{yyyy-MM-dd}-%i.log">
   <PatternLayout>
    <pattern>[%-5level] %d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %c{1} - %msg%n</pattern>
   </PatternLayout>
   <Policies>
    <TimeBasedTriggeringPolicy/>
    <SizeBasedTriggeringPolicy size="25 MB"/>
   </Policies>
   <DefaultRolloverStrategy max="10"/>
  </RollingFile>
 </Appenders>

 <Loggers>
  <Logger name="config" level="info"/>
  <Root level="info">
   <AppenderRef ref="RollingFile" level="info"/>
  </Root>
 </Loggers>
</Configuration>
EOF
fi


export AAV4PROXY_CLASSPATH=${SCRIPT_DIR}/jar/archapplv4proxy.jar:\
${SCRIPT_DIR}/lib/pbrawclient-0.0.5.jar:\
${SCRIPT_DIR}/lib/protobuf-java-2.4.1.jar:\
${SCRIPT_DIR}/lib/pvAccessJava-4.1.2.jar:\
${SCRIPT_DIR}/lib/pvDataJava-5.0.2.jar:\
${SCRIPT_DIR}/lib/log4j-api-2.2.jar:\
${SCRIPT_DIR}/lib/log4j-core-2.2.jar:\
${SCRIPT_DIR}/lib/guava-18.0.jar:\
${SCRIPT_DIR}/lib/json-simple-1.1.1.jar:\
${SCRIPT_DIR}/lib/commons-daemon-1.0.15.jar



case $COMMAND in
run)
#    export LOG_CONFIG_FILE=debug_log4jconfig.xml
	java \
		-Dlog4j.configurationFile=${LOG_CONFIG_FILE} \
		-DhelpFolder=${SCRIPT_DIR}/docs \
		-Xmx2G -XX:+UseG1GC \
		-ea \
		-cp ${AAV4PROXY_CLASSPATH} \
		org.epics.archiverappliance.v4service.V4ArchApplProxy \
		${SERVICE_NAME} \
		${APPLIANCE_URL}
	   	;;

start)
	${JSVC_BIN} \
		-Dlog4j.configurationFile=${LOG_CONFIG_FILE} \
		-DhelpFolder=${SCRIPT_DIR}/docs \
		-Xmx2G -XX:+UseG1GC \
		-cwd ${LOGS_FOLDER} \
		-pidfile ${LOGS_FOLDER}/${HOSTNAME}_archappl_v4proxy.pid \
		-outfile ${HOSTNAME}_archappl_v4proxy_stdout.log \
		-errfile ${HOSTNAME}_archappl_v4proxy_stderr.log \
		-ea \
		-cp ${AAV4PROXY_CLASSPATH} \
		org.epics.archiverappliance.v4service.V4ArchApplProxy \
		${SERVICE_NAME} \
		${APPLIANCE_URL}
	   	;;
stop)
	echo "Stopping the V4 archiver server"
	${JSVC_BIN} \
		-stop \
		-Dlog4j.configurationFile=${LOG_CONFIG_FILE} \
		-DhelpFolder=${SCRIPT_DIR}/docs \
		-Xmx2G -XX:+UseG1GC \
		-cwd ${LOGS_FOLDER} \
		-pidfile ${LOGS_FOLDER}/${HOSTNAME}_archappl_v4proxy.pid \
		-ea \
		-cp ${AAV4PROXY_CLASSPATH} \
		org.epics.archiverappliance.v4service.V4ArchApplProxy \
		${SERVICE_NAME} \
		${APPLIANCE_URL}
		;;
esac
