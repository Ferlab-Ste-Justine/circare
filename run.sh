#!/bin/bash

echo To run the program, call it like so: ./run.sh \<input directory\> \<mapping\> \<clinical data\>

echo $1
echo $2
echo $3
echo ${@:4}

sudo docker run -ti -v $PWD/$1:/workdir/input -v $PWD/$2:/workdir/input/mapping.csv -v $PWD/$3:/workdir/input/clinicalData.csv 0e1c67a64987 ${@:4}
