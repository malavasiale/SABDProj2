#!/bin/bash

mvn clean install package
cp target/*jar-with-dependencies.jar dist/data/preprocessing/.
cp target/*jar-with-dependencies.jar dist/data/query1/.
cp target/*jar-with-dependencies.jar dist/data/query2/.
cp target/*jar-with-dependencies.jar dist/data/query3/.
echo "Done!"
