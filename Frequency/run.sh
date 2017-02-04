#!/bin/bash

COUNTER=0
while [ $COUNTER -le 200 ]; do
   let freq=99900+COUNTER
   let COUNTER=COUNTER+1 
   java -cp target/com.pavelkazenin.frequency-1.0.jar com.pavelkazenin.frequency.ThetaDataMain  $freq 20  > thetaData
   echo -n "FS= " $freq
   spark-submit --class com.pavelkazenin.frequency.ThetaDataEtlMain target/com.pavelkazenin.frequency-1.0.jar 50 200 2>/dev/null
done | grep "FK ="


