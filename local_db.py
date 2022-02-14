#!/usr/bin/env python3
import os
import pickle
from glob import glob
from tqdm import tqdm
import config

def make_index (patterns=config.SERVED_DATA):
    paths = []
    for pat in patterns:
        paths.extend(glob(pat))
        pass
    lookup = {}
    for fid, path in enumerate(tqdm(paths)):
        with open(path, 'rb') as f:
            while True:
                try:
                    off = f.tell()
                    l = next(f)
                    pid = int(l[:30].split(b'\t')[0])
                    lookup[pid] = (fid, off)
                except StopIteration:
                    break
                pass
        break
        pass
    return paths, lookup

INDEX_PATH = 'local_db.index'

class DB:
    def __init__ (self):
        if os.path.exists(INDEX_PATH):
            with open(INDEX_PATH, 'rb') as f:
                paths, lookup = pickle.load(f)
                pass
            pass
        else:
            paths, lookup = make_index()
            with open(INDEX_PATH, 'wb') as f:
                pickle.dump((paths, lookup), f)
                pass
            pass
        self.paths = paths
        self.lookup = lookup
        pass

    def get (self, pid):
        fid, off = self.lookup[pid]
        path = self.paths[fid]
        with open(path, 'rb') as f:
            f.seek(off)
            return next(f)
        pass
    pass

default_db = None

def load_raw_case (pid):
    pid = int(pid)
    global default_db
    if default_db is None:
        default_db = DB()
        pass
    line = default_db.get(pid)
    pid2, line = line.split(b'\t')
    assert int(pid2) == pid
    return line

if __name__ == '__main__':
    db = DB()
    for k, _ in db.lookup.items():
        line = db.get(k)
        pid = int(line[:30].split(b'\t')[0])
        if pid != k:
            print(k)
            print(line)
            assert False
        print(pid)
        break


