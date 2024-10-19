#!/bin/bash

unzip $1 -d ./tmpin

source venv/bin/activate 
pip install -r requirements.txt > /dev/null

for txt_file in ./tmpin/*.txt
do
	txt_basename=$(basename "$txt_file" .txt)
	python csvddb.py -f "$txt_file" -t "$txt_basename" -d "$3" -s "$2/create_$txt_basename.sql" -v
done

deactivate

rm -R ./tmpin
