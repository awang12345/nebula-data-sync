package com.opensource.nebula.sync;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {

    private AtomicInteger count = new AtomicInteger(0);

    private String prefix;

    public NamedThreadFactory(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        String name = String.format("%s-%s", prefix, count.incrementAndGet());
        return new Thread(r, name);
    }
}
