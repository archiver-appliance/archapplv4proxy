#!/bin/bash

java -Dlog4j.configurationFile=debug_log4jconfig.xml -ea -Xmx2G -classpath build/classes:lib/pbrawclient-0.0.4.jar:lib/protobuf-java-2.4.1.jar:lib/pvDataJava-4.0.0.jar:lib/pvAccessJava-4.0.0.jar:lib/log4j-api-2.2.jar:lib/log4j-core-2.2.jar:lib/guava-18.0.jar org.epics.archiverappliance.v4service.V4ArchApplProxy archProxy http://localhost:17665/retrieval/data/getData.raw
