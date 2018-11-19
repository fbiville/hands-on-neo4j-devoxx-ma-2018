#!/usr/bin/env bash
set -euo pipefail

echo "Decrypting files"
git-crypt unlock ~/.secrets/devoxxma.txt

echo -n Neo4j password:
read -s password
mvn -q net.biville.florent.cypher:workshop-exporter-plugin:generate-cypher-file     -Dbolt-uri="bolt://localhost:7687"     -Dusername="neo4j"     -Dpassword="$password"     -Dexercise-input=src/main/resources/exercises/exercises.json     -Dcypher-output=src/main/resources/exercises/dump.cypher 2> /dev/null

echo "Encrypting files again"
git-crypt lock 
