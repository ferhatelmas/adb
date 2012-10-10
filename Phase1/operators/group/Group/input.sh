#!/bin/bash

hadoop fs -rm /user/cloudera/group/input/*

hadoop fs -put R.txt /user/cloudera/group/input
