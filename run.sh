#!/usr/bin/env bash
set -euo pipefail

cd target
unzip cypher-workshop.zip
./cypher-workshop/bin/cypher-workshop "$@"
