#!/bin/bash
export P=EkfoytBeoc
echo icdatasrv1-7
mysqladmin --host=icdatasrv1-7 --password=$P proc -v


echo icdatasrv1-18
mysqladmin --host=icdatasrv1-18 --password=$P proc -v


echo icdatasrv2-7
mysqladmin --host=icdatasrv2-7 --password=$P proc -v


echo icdatasrv2-18
mysqladmin --host=icdatasrv2-18 --password=$P proc -v


echo icdatasrv3-7
mysqladmin --host=icdatasrv3-7 --password=$P proc -v


echo icdatasrv3-18
mysqladmin --host=icdatasrv3-18 --password=$P proc -v


echo icdatasrv4-7
mysqladmin --host=icdatasrv4-7 --password=$P proc -v


echo icdatasrv4-18
mysqladmin --host=icdatasrv4-18 --password=$P proc -v



# Generated with: hosts = ["icdatasrv%d-%d" % (blade, machine) for blade in [1, 2, 3, 4] for machine in [7, 18]]
# print "\n".join(["echo %s\nmysqladmin --host=%s --password=$P proc -v\n\n" % (host, host) for host in hosts])
# 

echo SSH HEALTH
#print "\n".join(["ssh team7@%s 'hostname'\n\n" % (host, ) for host in hosts])
# ss
ssh team7@icdatasrv1-7 'hostname'


ssh team7@icdatasrv1-18 'hostname'


ssh team7@icdatasrv2-7 'hostname'


ssh team7@icdatasrv2-18 'hostname'


ssh team7@icdatasrv3-7 'hostname'


ssh team7@icdatasrv3-18 'hostname'


ssh team7@icdatasrv4-7 'hostname'


ssh team7@icdatasrv4-18 'hostname'

