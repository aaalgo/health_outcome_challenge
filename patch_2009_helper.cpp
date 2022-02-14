#include <fstream>
#include <iostream>
#include <unordered_map>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <glog/logging.h>

using namespace std;

void split (string const &line, vector<string> *fs) {
    boost::split(*fs,line, [](char c){return c == ',';});
}

void patch (string const &old_path,
            string const &patch_path,
            string const &out_path,
            string const &mapping_path) {
    int n_patch, n_old;
    int patch_key, old_key;
    int patch_pid, old_pid;
    vector<pair<int, int>> replaces;
    {
        ifstream mapping(mapping_path);
        mapping >> n_patch >> n_old >> patch_key >> old_key >> patch_pid >> old_pid;
        {
            int p, o;
            while (mapping >> p >> o) {
                replaces.emplace_back(p, o);
            }
        }
    }

    ifstream patch(patch_path);
    CHECK(patch);
    ifstream old(old_path);
    CHECK(old);
    ofstream out(out_path);
    LOG(INFO) << "Patching " << old_path << "...";
    int c = 0;
    string line;
    while (getline(old, line)) {
        boost::trim_right(line);
        vector<string> strs;
        split(line, &strs);
        CHECK(strs.size() == n_old);

        getline(patch, line);
        CHECK(patch);
        boost::trim_right(line);
        vector<string> patch_strs;
        split(line, &patch_strs);
        CHECK(patch_strs.size() == n_patch);

        CHECK(strs[old_key] == patch_strs[patch_key]);
        CHECK(strs[old_pid] == patch_strs[patch_pid]);

        for (auto p: replaces) {
            int patch_f = p.first;
            int old_f = p.second;
            strs[old_f].swap(patch_strs[patch_f]);
        }
        bool first = true;
        for (string const &s: strs) {
            if (first) first = false;
            else out << ',';
            out << s;
        }
        out << endl;
        c += 1;
        if (c % 100000 == 0) {
            LOG(INFO) << c << " lines loaded.";
        }
    }
}

int main (int argc, char *argv[]) {
    patch(argv[1], argv[2], argv[3], argv[4]);
    return 0;
}
