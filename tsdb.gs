package com.gschat.tsdb;

using gslang.Package;

@Package(Lang:"golang",Name:"com.gschat.tsdb",Redirect:"github.com/gschat/tsdb")

table DBValue {
    uint64 ID;
    byte[] Content;
}


table ValSpace {
    string Filename;
    uint64 Offset;
    uint32 Capacity;
    uint32 Header;
    uint32 Tail;
}
