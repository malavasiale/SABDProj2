#!/bin/bash

mvn clean install package
cp target/*jar-with-dependencies.jar dist/data/preprocessing/.
cp target/*jar-with-dependencies.jar dist/data/query1/.
echo "Done!"