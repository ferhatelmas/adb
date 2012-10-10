#!/bin/bash

	javac -nowarn -classpath /usr/lib/hadoop-0.20/hadoop-0.20.2-cdh3u0-core.jar -d group_classes GroupBy.java ../TextTriple.java

jar cvf group.jar -C group_classes/ . 

rm -R output
