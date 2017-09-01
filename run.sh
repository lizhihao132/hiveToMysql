#!/bin/sh

#!/bin/sh

if [ ! -f "HiveToMysql.class" ]||[ ! -f "HiveToMysql$1.class" ]; then
	javac -encoding utf-8 HiveToMysql.java
fi

java -Dfile.encoding=UTF-8 HiveToMysql dump.conf