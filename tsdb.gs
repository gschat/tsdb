package com.gschat.tsdb;

using gslang.Lang;

@Lang(Name:"golang",Package:"github.com/gschat/tsdb")

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
