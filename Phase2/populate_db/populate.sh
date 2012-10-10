#!/bin/bash

if [ $# -ne 1 ]
then
    echo "Usage: $0 <scale-factor>"
    exit 1
fi

#passwordless login isn't possible... even using ssh keys
read -sp "Password: " mypassword

echo "Starting with scale factor $1. Please enter password another eight times..."
ssh -f team7@icdatasrv1-7 /export/home/team7/populate_db/populate-partition.sh $1 0 $mypassword
ssh -f team7@icdatasrv2-7 /export/home/team7/populate_db/populate-partition.sh $1 1 $mypassword
ssh -f team7@icdatasrv3-7 /export/home/team7/populate_db/populate-partition.sh $1 2 $mypassword
ssh -f team7@icdatasrv4-7 /export/home/team7/populate_db/populate-partition.sh $1 3 $mypassword
ssh -f team7@icdatasrv1-18 /export/home/team7/populate_db/populate-partition.sh $1 4 $mypassword
ssh -f team7@icdatasrv2-18 /export/home/team7/populate_db/populate-partition.sh $1 5 $mypassword
ssh -f team7@icdatasrv3-18 /export/home/team7/populate_db/populate-partition.sh $1 6 $mypassword
ssh -f team7@icdatasrv4-18 /export/home/team7/populate_db/populate-partition.sh $1 7 $mypassword
