#!/bin/bash

haloop=/usr/local/haloop/bin/hadoop

$haloop fs -rmr /yuzhen/output
$haloop jar SSSP.jar SSSP /yuzhen/btc /yuzhen/output 0 15

$haloop fs -rmr /yuzhen/output
$haloop jar SSSP.jar SSSP /yuzhen/friendster /yuzhen/output 0 22

$haloop fs -rmr /yuzhen/output
$haloop jar SSSP.jar SSSP /yuzhen/twitter /yuzhen/output 0 15
