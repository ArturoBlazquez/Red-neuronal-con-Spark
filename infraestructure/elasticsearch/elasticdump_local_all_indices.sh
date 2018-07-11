INPUT=$1
DEST=$2

for file in $INPUT/mapping/*.json
do 
  elasticdump --input=$file --output=$DEST/$(basename "${file%.*}") --type=mapping
done

for file in $INPUT/data/*.json
do 
  elasticdump --input=$file --output=$DEST/$(basename "${file%.*}") --type=data --limit=5000
done