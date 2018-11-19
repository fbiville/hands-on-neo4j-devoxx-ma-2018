# Devoxx Morocco 2018 - Cypher workshop

## Getting started

 1. make sure the credentials are correct in `generate-importable-exercises.sh` (piece of advice: use git-crypt or read password from env)
 1. run `generate-importable-exercises.sh`
 1. run `mvn package`
 1. run `./run.sh` with the relevant parameters, e.g.
```shell
 $ ./run.sh -p
[[[ interactively prompts for your Neo4j password ]]]
```

## Defining exercises

It is as easy as changing `src/main/resources/exercises.json`
and re-running `generate-importable-exercises.sh`.
