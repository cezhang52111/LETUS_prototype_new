cd ../
./build.sh --build-type release


cd exps/
rm -rf data/
mkdir data/
# ${PWD}/../build_debug/bin/ycsb_simple 
${PWD}/../build_release/bin/my_test -filepath readwriteeven-uniform-data.txt -resultpathbase readwriteeven-uniform -basetxnum 20000 -txnum 20000000