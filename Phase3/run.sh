#!/bin/sh
java -cp "lib/mysql-connector-java-5.1.19-bin.jar:lib/bcprov-jdk15on-147.jar:lib/slf4j-api-1.6.4.jar:lib/bcpkix-jdk15on-147.jar:lib/slf4j/slf4j-simple-1.6.4.jar:lib/jzlib.jar:lib/sshj.jar:buildcube_test.jar" Main "$@"
