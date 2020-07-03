#!/bin/bash
rm -rf /tmp/final_project
mkdir /tmp/final_project/
gcc -o pre_aggregation.o pre_aggregation.c -I /usr/include/postgresql -lpq
./pre_aggregation.o
gcc -o reader.o reader.c -I /usr/include/postgresql -lpq
# index=0
# while [ $index -le 0 ]
while true;
do
    wget http://loh.istgahesalavati.ir/report.gz.tar
    tar -xvf report.gz.tar
    rm report.gz.tar
    mv *.text /tmp/final_project
    ./reader.o
    sleep 60
    # index=`expr $index + 1`
done
