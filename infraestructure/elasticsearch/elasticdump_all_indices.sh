INPUT=$1
DEST=$2
indices=$(curl -s -XGET $INPUT/_cat/indices?h=i&health=green)
for INDEX in $indices
do
  JSON={\"query\":{\"match_all\":{}},\"_source\":{\"includes\":[\"@timestamp\",\"ticket.cashAmount\",\"ticket.operationCode\",\"ticket.agentNumericCode\"]}}
  elasticdump --input=$INPUT/$INDEX --output=$DEST/$INDEX --type=mapping #--searchBody=$JSON
  elasticdump --input=$INPUT/$INDEX --output=$DEST/$INDEX --type=data --limit=5000 #--searchBody=$JSON
done