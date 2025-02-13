package com.google.edwmigration.dbsync.client;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import joptsimple.ValueConversionException;
import joptsimple.ValueConverter;

public class InetSocketAddressConverter implements ValueConverter<InetSocketAddress> {

    private final int defaultPort;

    public InetSocketAddressConverter(int defaultPort) {
        this.defaultPort = defaultPort;
    }

    @Override
    public InetSocketAddress convert(String value) {
        int idx = value.lastIndexOf(':');
        if (idx == -1) {
            return InetSocketAddress.createUnresolved(value, defaultPort);
        }
        try {
            int port = Integer.parseInt(value.substring(idx + 1));
            return InetSocketAddress.createUnresolved(value.substring(0, idx), port);
        } catch (NumberFormatException e) {
            throw new ValueConversionException("Cannot parse port from '" + value + "'", e);
        }
    }

    @Override
    public Class<? extends InetSocketAddress> valueType() {
        return InetSocketAddress.class;
    }

    @Override
    public String valuePattern() {
        return "<host/addr>[:<port>]";
    }
}
