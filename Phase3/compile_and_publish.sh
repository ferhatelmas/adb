#!/bin/sh
# our code
cd ./dpch_team7/src/
javac -classpath "../../lib/mysql-connector-java-5.1.19-bin.jar:../../lib/bcprov-jdk15on-147.jar:../../lib/slf4j-api-1.6.4.jar:../../lib/bcpkix-jdk15on-147.jar:../../lib/slf4j/slf4j-simple-1.6.4.jar:../../lib/jzlib.jar:../../lib/sshj.jar:." -d buildcube_classes */*/*.java */*.java Main.java
jar -cvmf META-INF/MANIFEST.MF buildcube.jar -C buildcube_classes/ .
scp ./buildcube.jar team7@icdatasrv2:/export/home/team7/buildcube_test.jar
cd -
cp ./dpch_team7/src/buildcube.jar ./buildcube_test.jar

#../../lib/mysql-connector-java-5.1.19-bin.jar:../../lib/bcprov-jdk15on-147.jar:../../lib/slf4j-api-1.6.4.jar:../../lib/bcpkix-jdk15on-147.jar:../../slf4j-simple-1.6.4.jar:.

# "contrib/contrib.jar:lib/mysql-connector-java-5.1.19-bin.jar:lib/bcprov-jdk15on-147.jar:lib/slf4j-api-1.6.4.jar:lib/bcpkix-jdk15on-147.jar:lib/slf4j/slf4j-simple-1.6.4.jar:.lib/jzlib.jar:lib/sshj.jar:.:lib/mysql-connector-java-5.1.19-bin.jar:./buildcube.jar"
