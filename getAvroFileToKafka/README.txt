// Get the schema
java -jar avro-tools-1.10.2.jar getschema users_generic.avro > schema.avsc

// Get the documents without schema
java -jar avro-tools-1.10.2.jar tojson users_generic.avro > docToKafka.json

// Run the following command to convert the schema to a string and ensure the speech marks are not escaped.
jq -r tostring schema.avsc

// You are just checking the output here.

// Create a topic to store the avro documents:
./kafka-topics ./kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic userdata

// Publish the data:
e.g 1
~/confluent/bin/./kafka-avro-console-producer --bootstrap-server localhost:9092 --topic userdata --property schema.registry.url=http://localhost:8081 --property value.schema="$(jq -r tostring schema.avsc)" < docToKafka.json

e.g 2
~/confluent/bin/./kafka-avro-console-producer --bootstrap-server localhost:9092 --topic transactionData --property schema.registry.url=http://localhost:8081 --property value.schema="$(jq -r tostring transactions.avsc)" < transactionsToKafka.json


// See also https://docs.confluent.io/platform/current/schema-registry/develop/using.html
Get the schema's registered
curl --silent -X GET http://localhost:8081/subjects | jq

// Get the latest scheme version and version number
curl --silent -X GET http://localhost:8081/subjects/userdata-value/versions/latest | jq

// Delete schema version 1
curl -X DELETE http://localhost:8081/subjects/transactionData-value/versions/1

// Get avro data from topic
~/confluent/bin/./kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic userdata --from-beginning --property schema.registry.url=http://localhost:8081
