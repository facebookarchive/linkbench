package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;

public enum AssocVisibility
{
VISIBLE(0), DELETED(1), UNUSED1(2), HIDDEN(3), HARD__DELETE(4);

  private final int value;

AssocVisibility(int value)
  {
    this.value = value;
  }

  @ThriftEnumValue
  public int getValue()
  {
    return value;
  }
}
