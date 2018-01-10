/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tests.snapshot;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.PriorityBlockingQueue;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This class verifies the output windows of the job
 * using a priority blocking queue.
 * De-duplicates the windows and in case of a missing one
 * waits for a fixed amount of time and throws assertion error
 */
public class QueueVerifier extends Thread implements Closeable {

    private static final long TIMEOUT = 300_000;

    private static final int INITIAL_QUEUE_SIZE = 10_000;

    private final PriorityBlockingQueue<Long> queue;

    private final int totalWindowCount;

    private int windowCount;

    private long key;

    private long lastCheck = -1;

    private volatile boolean running = true;

    public QueueVerifier(int windowCount) {
        this.queue = new PriorityBlockingQueue<>(INITIAL_QUEUE_SIZE);
        this.totalWindowCount = windowCount;
        this.windowCount = windowCount;
    }

    public void offer(long item) {
        if (!running) {
            throw new AssertionError("Failed at key: " + key +
                    ", remaining window count: " + windowCount + ", total window count per key: " + totalWindowCount);
        }
        queue.offer(item);
    }

    @Override
    public void run() {
        while (running) {
            Long next = queue.peek();
            if (next == null) {
                //Queue is empty, sleep
                sleepSeconds(1);
            } else if (next == key) {
                //Happy path
                queue.poll();
                lastCheck = -1;
                if (--windowCount == 0) {
                    //we have enough windows for this key, increment the key
                    key++;
                    windowCount = totalWindowCount;
                }
            } else if (next < key) {
                //we have a duplicate
                queue.poll();
            } else if (lastCheck == -1) {
                //mark last check for timeout
                lastCheck = System.currentTimeMillis();
            } else if ((System.currentTimeMillis() - lastCheck) > TIMEOUT) {
                //time is up
                running = false;
            } else {
                //sleep for timeout
                sleepSeconds(1);
                System.out.println("Verified: " + key);
            }
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
    }

    private static void sleepSeconds(int seconds) {
        uncheckRun(() -> SECONDS.sleep(seconds));
    }
}
