cd ../
./build.sh --build-type release


cd exps/
rm -rf data/
mkdir data/
# ${PWD}/../build_debug/bin/ycsb_simple 
${PWD}/../build_release/bin/my_test -filepath readonly-uniform-data.txt -resultpathbase readonly-uniform -basetxnum 20000 -txnum 100000