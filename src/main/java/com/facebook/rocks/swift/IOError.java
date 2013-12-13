package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

@ThriftStruct("IOError")
public class IOError extends Exception
{
    private static final long serialVersionUID = 1L;

    @ThriftConstructor
    public IOError(
        @ThriftField(value=1, name="message") final String message
    ) {
        this.message = message;
    }

    private final String message;

    @ThriftField(value=1, name="message")
    public String getMessage() { return message; }
}
