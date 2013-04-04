package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;

public enum CompressionType
{
K_NO_COMPRESSION(0), K_SNAPPY_COMPRESSION(1), K_ZLIB(2), K_BZIP2(3);

  private final int value;

CompressionType(int value)
  {
    this.value = value;
  }

  @ThriftEnumValue
  public int getValue()
  {
    return value;
  }
}
