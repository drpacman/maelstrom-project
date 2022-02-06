#!/bin/bash -ex
cd $(dirname $0)/..
echo Test Echo workload
./maelstrom test -w echo --bin target/debug/echo --time-limit 10

echo Test Broadcast workload
./maelstrom test -w broadcast --bin target/debug/broadcast --time-limit 20 --topology tree4 --nemesis partition

echo Test GSet CRDT workload
./maelstrom test -w g-set --bin target/debug/gset --time-limit 20 --rate 10

echo Test Counter CRDT workload
./maelstrom test -w pn-counter --bin target/debug/gcounter --time-limit 30 --rate 10 --nemesis partition

echo Test Datomic workload
./maelstrom test -w txn-list-append --bin target/debug/datomic --time-limit 10 --node-count 10 --rate 100