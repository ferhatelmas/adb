#!/bin/bash

hadoop fs -rmr /user/cloudera/group/output

hadoop jar group.jar operators.group.Group.GroupBy /user/cloudera/group/input/R.txt 1 2 3 0 /user/cloudera/group/output

hadoop fs -cat /user/cloudera/group/output/part-00000
