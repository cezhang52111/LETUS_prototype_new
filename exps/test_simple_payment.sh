cd ../
./build.sh --build-type release


cd exps/
rm -rf data/
mkdir data/
# ${PWD}/../build_debug/bin/ycsb_simple 
${PWD}/../build_release/bin/simple_payment