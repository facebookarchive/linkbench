package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;

public enum Code
{
K_OK(0), K_END(1), K_NOT_FOUND(2), K_CORRUPTION(3), K_NOT_SUPPORTED(4), K_INVALID_ARGUMENT(5), K_IOERROR(6), K_SNAPSHOT_NOT_EXISTS(7), K_WRONG_SHARD(8), K_UNKNOWN_ERROR(9), K_CLIENT_ERROR(10);

  private final int value;

Code(int value)
  {
    this.value = value;
  }

  @ThriftEnumValue
  public int getValue()
  {
    return value;
  }
}
