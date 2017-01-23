package org.apache.asterix.active;

public class StateSubscriber {
    private final byte state;
    private boolean done = false;

    public StateSubscriber(byte state) {
        this.state = state;
    }

    public synchronized void notify(byte state) {
        if (this.state == state || this.state == ActivityState.FAILED) {
            done = true;
        }
        notifyAll();
    }

    public boolean done() {
        return done;
    }

    public synchronized void complete() throws InterruptedException {
        while (!done) {
            wait();
        }
    }
}