#!/bin/sh

# Run to copy populate script from current server (2) to all others' home directories.

if [ `hostname` != icdatasrv2 ]
then
    echo "Must be run from icdatasrv2; copies data FROM there OVERWRITING all other data!"
    exit 1
fi
if [ `pwd` != /export/home/team7/populate_db ]
then
    echo "Must be run from /export/home/team7/populate_db!"
    exit 1
fi

ssh team7@icdatasrv1 rm -rf /export/home/team7/populate_db
ssh team7@icdatasrv3 rm -rf /export/home/team7/populate_db
ssh team7@icdatasrv4 rm -rf /export/home/team7/populate_db
scp -pr /export/home/team7/populate_db team7@icdatasrv1:/export/home/team7/populate_db
scp -pr /export/home/team7/populate_db team7@icdatasrv3:/export/home/team7/populate_db
scp -pr /export/home/team7/populate_db team7@icdatasrv4:/export/home/team7/populate_db
