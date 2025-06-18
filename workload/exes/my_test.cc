/**
 * A very simplified YCSB workload
 *  - only have one table, the table only have one field/column
 */

#include <fstream>
#include <iostream>
#include <string>
#include <utility>
#include <tuple>
#include <chrono>
#include "DMMTrie.hpp"
#include "LSVPS.hpp"
#include "kv_buffer.hpp"
#include "properties.hpp"
#include "workload.hpp"
#include "workload_utilis.hpp"
#include <regex>
using namespace std;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
void ParseCommandLine(int argc, const char *argv[], string &filepath,
                      string &result_path_base, string &base_tx_num, string &tx_num);

bool TransactionRead(DMMTrie *trie, uint64_t version, const string &key,
                     string &value, KVBuffer &unverified_keys) {
#ifdef DEBUG
  cout << "[TransactionRead]";
  cout << "version: " << version << ",";
  cout << "key: " << key << endl;
#endif
  value = trie->Get(0, version, key);
  unverified_keys.Put(version, key, value);
  return true;
} 

bool TransactionUpdate(DMMTrie *trie, uint64_t version, const string &key,
                       const string &value, KVBuffer &unverified_keys) {
#ifdef DEBUG
  cout << "[TransactionUpdate]";
  cout << "version: " << version << ",";
  cout << "key: " << key << ",";
  cout << "value: " << value << endl;
#endif
  trie->Put(0, version, key, value);
  unverified_keys.Put(version, key, value);
  return true;
}
bool TransactionInsert(DMMTrie *trie, uint64_t version, const string &key,
                       const string &value, KVBuffer &unverified_keys) {
#ifdef DEBUG
  cout << "[TransactionInsert]";
  cout << "version: " << version << ",";
  cout << "key: " << key << ",";
  cout << "value: " << value << endl;
#endif
  trie->Put(0, version, key, value);
  unverified_keys.Put(version, key, value);
  return true;
}
bool TransactionScan(DMMTrie *trie, uint64_t version, const string &key,
                     uint64_t len, KVBuffer &unverified_keys) {
#ifdef DEBUG
  cout << "[TransactionScan]";
  cout << "version: " << version << ",";
  cout << "key: " << key << ",";
  cout << "len: " << len << endl;
#endif
  for (int i = 0; i < len; i++) {
    string k = to_string(stoul(key) + i);
    string v = trie->Get(0, version, k);
    unverified_keys.Put(version, k, v);
  }
  return true;
}

bool TransactionReadModifyWrite(DMMTrie *trie, uint64_t version,
                                const string &key, string &value,
                                KVBuffer &unverified_keys) {
#ifdef DEBUG
  cout << "[TransactionReadModifyWrite]";
  cout << "version: " << version << ",";
  cout << "key: " << key << ",";
  cout << "value: " << value << endl;
#endif
  string value_read = trie->Get(0, version, key);
  unverified_keys.Put(version, key, value_read);
  trie->Put(0, version + 1, key, value);
  unverified_keys.Put(version + 1, key, value);
  value = value_read;
  return true;
}

uint64_t loading(DMMTrie *trie, Workload *wl) {
  int num_op = wl->GetRecordCount();
  int batch_size = wl->GetBatchSize();
  uint64_t version = 1;
  int i;
  for (i = 0; i < num_op; i++) {
    trie->Put(0, version, wl->NextSequenceKey(), wl->NextRandomValue());
    if (i && i % batch_size == 0) {
      trie->Commit(version);
      version++;
    }
  }
  if ((i - 1) % batch_size) {
    trie->Commit(version);
  }
  return version;
}

void transaction(DMMTrie *trie, Workload &wl, uint64_t version,
                 KVBuffer &unverified_keys) {
  int put_count = 0;
  int num_op = wl.GetOperationCount();
  int batch_size = wl.GetBatchSize();
  uint64_t current_version = version;
  bool status;
  uint64_t ver;
  string k, v;
  for (int i = 0; i < num_op; i++) {
    switch (wl.NextOperation()) {
      case READ:
        ver = current_version;
        k = wl.NextTransactionKey();
        v = "";
        status = TransactionRead(trie, ver, k, v, unverified_keys);
        break;
      case UPDATE:
        ver = current_version + 1;
        k = wl.NextTransactionKey();
        v = wl.NextRandomValue();
        status = TransactionUpdate(trie, ver, k, v, unverified_keys);
        put_count += 1;
        break;
      case INSERT:
        ver = current_version + 1;
        k = wl.NextSequenceKey();
        v = wl.NextRandomValue();
        put_count += 1;
        status = TransactionInsert(trie, ver, k, v, unverified_keys);
        break;
      case SCAN:
        ver = current_version;
        k = wl.NextTransactionKey();
        status =
            TransactionScan(trie, ver, k, wl.NextScanLength(), unverified_keys);
        break;
      case READMODIFYWRITE:
        status = TransactionReadModifyWrite(trie, ver, k, v, unverified_keys);
        break;
      default:
        throw utils::Exception("Operation request is not recognized!");
    }
    if (put_count && put_count % batch_size == 0) {
      // TODO: unverfied keys is only readable after commit
      current_version++;
      trie->Commit(current_version);
      put_count = 0;
#ifdef DEBUG
      cout << "[commit] version:" << current_version << endl;
#endif
    }
  }
  if (put_count) {
    // TODO: unverfied keys is only readable after commit
    current_version++;
    trie->Commit(current_version);
#ifdef DEBUG
    cout << "[commit] version:" << current_version << endl;
#endif
  }
}

void verification(DMMTrie *trie, KVBuffer &unverified_keys) {
  vector<tuple<uint64_t, string, string>> results = unverified_keys.Read();
  for (auto result : results) {
    uint64_t version = get<0>(result);
    auto key = get<1>(result);
    auto value = get<2>(result);
    string root_hash = trie->GetRootHash(0, version);
    DMMTrieProof proof = trie->GetProof(0, version, key);
    bool vs = trie->Verify(0, key, value, root_hash, proof);
    // TODO: verify
#ifdef DEBUG
    cout << "[verification]";
    cout << "ver: " << version << ", ";
    cout << "key: " << key << ", ";
    cout << "value: " << value << ", ";
    cout << "verified? " << (vs ? "true" : "false") << endl;
#endif
  }
}

vector<string> extractString(const string& input) {
  vector<string> results;
  regex regexwrite("^INSERT usertable user(\\w+) \\[ field\\d+=(.+) \\]$");
  regex regexupdate("^UPDATE usertable user(\\w+) \\[ field\\d+=(.+) \\]$");
  regex regexread("^READ usertable user(\\w+) \\[.+\\]$");
  smatch matchwrite;
  smatch matchupdate;
  smatch matchread;
  if (regex_search(input, matchwrite, regexwrite)) {
    results.push_back("INSERT");
    for (size_t i = 1; i < matchwrite.size(); ++i) {
      results.push_back(matchwrite[i].str());
    }
  } else if (regex_search(input, matchupdate, regexupdate)) {
    results.push_back("UPDATE");
    for (size_t i = 1; i < matchupdate.size(); ++i) {
      results.push_back(matchupdate[i].str());
    }
  } else if (regex_search(input, matchread, regexread)) {
    results.push_back("READ");
    results.push_back(matchread[1].str());
  } else {
    results.push_back("");
  }
  return results;
}

string exec(const char* cmd) {
    shared_ptr<FILE> pipe(popen(cmd, "r"), pclose);
    if (!pipe) return "ERROR";
    char buffer[128];
    string result = "";
    while (!feof(pipe.get())) {
        if (fgets(buffer, 128, pipe.get()) != NULL)
            result += buffer;
    }
    return result;
}

vector<string> split_string(string input, char delimiter) {
  string s;
  stringstream ss(input);
  vector<string> v;
  while (getline(ss, s, delimiter)) {
    v.push_back(s);
  }
  return v;
}

long long get_disk_size(string data_path) {
  string command_disk_space = "du -s -b "+data_path;
  string exec_result = exec(command_disk_space.c_str());
  vector<string> split_elems = split_string(exec_result, '\n');
  string size_str = split_string(split_elems.back(), '\t')[0];
  long long size = stoll(size_str);
  return size;
}

int main(const int argc, const char *argv[]) {
  string data_path = "data/";
  string index_path = "data/";
  string result_path_base = "";
  string filepath = "";
  // init database
  LSVPS *page_store = new LSVPS(index_path);
  VDLS *value_store = new VDLS(data_path);
  DMMTrie *trie = new DMMTrie(0, page_store, value_store);
  page_store->RegisterTrie(trie);
  uint64_t tid = 0;
  int block_tx_num = 100;
  string base_tx_num = "";
  string tx_num = "";
  int storage_capture_block_interval = 100;
  int flush_interval = 100;

  // simple test for the write
  /* trie->Put(tid, 1, "12345", "aaa");
  trie->Put(tid, 1, "23456", "bbb");
  trie->Put(tid, 1, "34567", "ccc");
  trie->Put(tid, 1, "45678", "ddd");
  trie->Put(tid, 1, "56789", "eee");
  trie->Commit(1);
  trie->Put(tid, 2, "11111", "aa2");
  trie->Put(tid, 2, "23456", "bb2");
  trie->Put(tid, 2, "34567", "cc2");
  trie->Put(tid, 2, "45678", "dd2");
  trie->Put(tid, 2, "56789", "ee2");
  trie->Commit(2);
  trie->Put(tid, 3, "11111", "aa3");
  trie->Put(tid, 3, "23456", "bb3");
  trie->Commit(3);
  string test_value = trie->Get(tid, 3, "34567");
  cout << "test_value " << test_value <<endl;
  DMMTrieProof proof = trie->GetProof(tid, 2, "23456");
  string root_hash2 = trie->GetRootHash(tid, 2);
  cout << boolalpha << trie->Verify(tid, "23456", "bb2", root_hash2, proof)
       << endl;
  cout << "proof size: " << proof.serial_size() <<endl;
  
  DMMTrieProof proof2 = trie->GetProof(tid, 1, "23456");     
  string root_hash1 = trie->GetRootHash(tid, 1);
  cout << boolalpha << trie->Verify(tid, "23456", "bbb", root_hash1, proof2)
       << endl;
  cout << boolalpha << trie->Verify(tid, "23456", "bb2", root_hash1, proof2)
       << endl;
  cout << "proof size: " << proof2.serial_size() <<endl;
 */

  ParseCommandLine(argc, argv, filepath, result_path_base, base_tx_num, tx_num);
  cout << base_tx_num << endl;
  int base_tx_num_int = stoi(base_tx_num);
  int tx_num_int = stoi(tx_num);
  cout << "file path: "<<filepath << endl;
  cout << "result path base: " << result_path_base << endl;
  cout << "base tx num: " << base_tx_num_int << endl;
  cout << "tx num: " << tx_num_int << endl;
  string timestamp_file = result_path_base + "-ts.json";
  cout << "timestamp file: " << timestamp_file << endl;
  string storage_file = result_path_base + "-storage.json";
  cout << "storage file: " << storage_file << endl;
  // read data file
  ifstream datafile(filepath);
  // write ts file
  ofstream tsfile(timestamp_file);
  // write storage file
  ofstream storagefile(storage_file);
  // string to store each line of the file
  string line;
  uint64_t cur_version = 1;

  if (datafile.is_open()) {
    vector<tuple<string, string, string>> block; // init the block info
    // read each line from the file and store it in the 'line' variable
    // build base
    int cnt = 0;
    while (cnt < base_tx_num_int) {
      getline(datafile, line);
      vector<string> results = extractString(line);
      if (results[0] == "INSERT") {
        block.push_back(tuple(results[0], results[1], results[2]));
        cnt += 1;
      } else {
        continue;
      }
      if (block.size() == block_tx_num) {
        // process the operations in block
        for (auto elem:block) {
          string key = get<1>(elem);
          string value = get<2>(elem);
          trie->Put(tid, cur_version, key, value);
        }
        trie->Commit(cur_version);
        if (cur_version % flush_interval == 0) {
          trie->Flush(tid, cur_version);
        } 
        // trie->Flush(tid, cur_version);
        cout << "block " << cur_version <<endl;
        block.clear();
        cur_version += 1;
      }
    }
    // handle the rest of the tx in the block
    if (block.size() > 0) {
      // process the operations in block
        for (auto elem:block) {
          string key = get<1>(elem);
          string value = get<2>(elem);
          trie->Put(tid, cur_version, key, value);
        }
        trie->Commit(cur_version);
        trie->Flush(tid, cur_version);
        cout << "block " << cur_version <<endl;
        block.clear();
        cur_version += 1;
    }
    cout << "build finish" <<endl;
    cnt = 0;
    while (cnt < tx_num_int) {
      getline(datafile, line);
      vector<string> results = extractString(line);
      if (results[0] == "INSERT" || results[0] == "UPDATE") {
        block.push_back(tuple(results[0], results[1], results[2]));
        cnt += 1;
      } else if (results[0] == "READ") {
        block.push_back(tuple(results[0], results[1], ""));
        cnt += 1;
      }
      if (block.size() == block_tx_num) {
        // process the operations in block
        auto start = chrono::high_resolution_clock::now();
        for (auto elem:block) {
          string op = get<0>(elem);
          if (op == "INSERT" || op == "UPDATE") {
            string key = get<1>(elem);
            string value = get<2>(elem);
            // trie->Get(tid, cur_version, key); 
            trie->Put(tid, cur_version, key, value);
          } else if (op == "READ") {
            string key = get<1>(elem);
            string value = trie->Get(tid, cur_version - 1, key);
            assert(value.length() >= 0);
          }
        }
        trie->Commit(cur_version);
        // trie->Flush(tid, cur_version);
        if (cur_version % flush_interval == 0) {
          trie->Flush(tid, cur_version);
        }
        auto finish = std::chrono::high_resolution_clock::now();
        auto elapse = chrono::duration_cast<std::chrono::nanoseconds>(finish-start).count(); // in nano second
        tsfile << "{\"block_id\":" << cur_version <<",\"elapse\":" << elapse << "}"<<endl;
        if ((cur_version - base_tx_num_int/block_tx_num) % storage_capture_block_interval == 0) {
          long long storage_size = get_disk_size(data_path);
          storagefile << "{\"block_id\":" << cur_version <<",\"size\":" << storage_size << "}"<<endl;
        }
        cout << "block " << cur_version <<endl;
        block.clear();
        cur_version += 1;
      }
    }
    // handle the rest of the tx in the block
    if (block.size() > 0) {
      // process the operations in block
      auto start = chrono::high_resolution_clock::now();
        for (auto elem:block) {
          string key = get<1>(elem);
          string value = get<2>(elem);
          trie->Put(tid, cur_version, key, value);
        }
        trie->Commit(cur_version);
        trie->Flush(tid, cur_version);
        auto finish = std::chrono::high_resolution_clock::now();
        auto elapse = chrono::duration_cast<std::chrono::nanoseconds>(finish-start).count(); // in nano second
        tsfile << "{\"block_id\":" << cur_version <<",\"elapse\":" << elapse << "}"<<endl;
        long long storage_size = get_disk_size(data_path);
        storagefile << "{\"block_id\":" << cur_version <<",\"size\":" << storage_size << "}"<<endl;
        cout << "block " << cur_version <<endl;
        block.clear();
        cur_version += 1;
    }
    // close the file stream
    datafile.close();
    tsfile.close();
    storagefile.close();
  }
  else {
    cerr << "Unable to open data file " << filepath << endl;
  }
}

void ParseCommandLine(int argc, const char *argv[], string &filepath,
                      string &result_path_base, string &base_tx_num, string &tx_num) {
  int argindex = 1;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-filepath") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filepath = argv[argindex];
      argindex++;
    } else if (strcmp(argv[argindex], "-resultpathbase") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      result_path_base = argv[argindex];
      argindex++;
    } else if (strcmp(argv[argindex], "-basetxnum") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      base_tx_num = argv[argindex];
      argindex++;
    } else if (strcmp(argv[argindex], "-txnum") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      tx_num = argv[argindex];
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "options:" << endl;
  cout << "  -datapath: directory where data is stored" << endl;
  cout << "  -idxpath: directory where index is stored" << endl;
  cout << "  -respath: directory where result is stored" << endl;
  cout << "  -confpath: path of configure file" << endl;
}

bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}