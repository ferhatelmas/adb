To populate DBs:

1) copy dbgen_src to icdatasrv2, cd to this directory and type make
2) copy the populate_db folder to /export/home/team7/populate_db, log in and cd there
3) cp ../dbgen_src/dbgen .
4) ./copy.sh
5) ./populate.sh, read the usage, and call again with args

dbgen_src: my modified version to do partitioning

populate_db: scripts for... you guessed it!
    copy.sh: copies the required stuff to other servers
    populate.sh: do population for some scale factor
    populate-partition.sh: populate a certain partition. Called by populate.sh on each node.

Note: you will have to enter password a lot unless you can hack the servers somehow. In theory one should be able to put ssh keys in the .ssh/ dirs, but those are root-owned and not editable... or were when I checked. Looks like someone has fixed this!
