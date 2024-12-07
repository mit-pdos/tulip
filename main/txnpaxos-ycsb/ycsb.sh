#!/bin/bash

if [ -z "$1" ]
then
	nruns=1
else
	nruns=$1
fi

dir=./exp
# rm -rf $dir
mkdir -p $dir

duration=10
rkeys=1000000

nkeys=4

for rdratio in 0 100
do
	for theta in 0.2
	do
		fpath=$dir/txnpaxos-$rdratio-$theta.csv
		rm -f $fpath
		for i in $(seq $nruns)
		do
			for nthrds in 1 2 4 8 16 32 64
			# for nthrds in 4
			do
				stdbuf -o 0 ./txnpaxos-ycsb -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -exp | tee -a $fpath
			done
		done
	done
done
