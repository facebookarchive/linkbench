package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import com.facebook.swift.service.*;
import java.io.*;
import java.util.*;

@ThriftService("RocksServiceCommon")
public interface RocksServiceCommon extends Closeable
{
    void close();

}