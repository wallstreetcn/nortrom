package com.wallstreetcn.flume;

class JDBCSinkException extends RuntimeException {
    JDBCSinkException(String e) {
        super(e);
    }

    JDBCSinkException(Exception e) {
        super(e);
    }
}
