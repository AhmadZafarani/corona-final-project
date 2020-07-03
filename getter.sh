#!/bin/bash
# rm -rf /tmp/final_project
index=0
while [ $index -le 1 ]
do
    wget http://loh.istgahesalavati.ir/report.gz.tar
    tar -xvf report.gz.tar
    rm report.gz.tar
    mkdir /tmp/final_project/
    mv *.text /tmp/final_project
    sleep 60
    index=`expr $index + 1`
done
# ls /tmp/final_project
