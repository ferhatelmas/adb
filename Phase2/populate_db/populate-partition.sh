#!/bin/bash
# local part of generation

if [ $# -ne 3 ]
then
    echo "Usage: $0 <scale-factor> <partition-number> <password>"
    exit 1
fi

cd /export/home/team7/populate_db
rm -f  supplier.tbl customer.tbl orders.tbl lineitem.tbl part.tbl partsupp.tbl nation.tbl region.tbl
./dbgen -s $1 -p $2
if [ $? -neq 0 ]
then
    exit $?
fi
mysql --password=$3 --database=dbcourse1 --local-infile=1 <import.sql
if [ $? -neq 0 ]
then
    exit $?
fi
rm -f  supplier.tbl customer.tbl orders.tbl lineitem.tbl part.tbl partsupp.tbl nation.tbl region.tbl
echo "Success!"
