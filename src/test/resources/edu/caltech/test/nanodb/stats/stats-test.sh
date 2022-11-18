#!/bin/bash

cd ../../../../../../../../

tmp=$(./nanodb < schemas/stores/make-stores.sql)

tmp=$(./nanodb < schemas/stores/stores-28K.sql)

tmp=$(./nanodb < schemas/stores/analyze-stats.sql)

./nanodb < schemas/stores/show-stats.sql > src/test/resources/edu/caltech/test/nanodb/stats/tmp

cd src/test/resources/edu/caltech/test/nanodb/stats

echo ""
echo "=== Your output ==="
cat tmp

echo ""
echo "=== Ans output ==="
cat ans

echo ""
echo "=== Diff ==="

diff --color=auto ans tmp
