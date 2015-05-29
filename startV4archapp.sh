#!/bin/bash

java -Dlog4j.configurationFile=debug_log4jconfig.xml -ea -Xmx2G -classpath build/classes:lib/pbrawclient-0.0.3.jar:lib/protobuf-java-2.4.1.jar:lib/pvDataJava-4.0.0.jar:lib/pvAccessJava-4.0.0.jar:lib/log4j-api-2.2.jar:lib/log4j-core-2.2.jar:lib/guava-18.0.jar org.epics.archiverappliance.v4service.V4ArchApplProxy archProxy http://test-arch.slac.stanford.edu:17668/retrieval/data/getData.raw
