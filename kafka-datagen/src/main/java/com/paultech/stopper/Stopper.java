package com.paultech.stopper;

public interface Stopper {
    long AWAIT_TIME_OUT = 3000L;
    void stop();
}
