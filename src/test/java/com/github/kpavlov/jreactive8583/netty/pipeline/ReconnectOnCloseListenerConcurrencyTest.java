package com.github.kpavlov.jreactive8583.netty.pipeline;

import com.github.kpavlov.jreactive8583.client.Iso8583Client;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Terminal invariant for {@link ReconnectOnCloseListener}:
 * when a channel close and an explicit {@code disconnectAsync()} happen
 * concurrently for the same connection, at most one reconnect task is
 * scheduled for that close event.
 *
 * <p>Synchronization is done exclusively with {@link CountDownLatch}s - no
 * sleeps and no timing assumptions. A {@link GatedCloseListener} parks the
 * close-handler thread at exact points in
 * {@code ReconnectOnCloseListener.operationComplete()} /
 * {@code scheduleReconnect()} (ReconnectOnCloseListener.kt:28-43), so both
 * interleavings are exercised deterministically. A recording
 * {@link ScheduledExecutorService} captures the {@code schedule(Callable)}
 * calls without executing them; it creates no threads and never invokes
 * {@code client.connectAsync()}, so the test leaves no channel, thread or
 * listening port behind.
 */
class ReconnectOnCloseListenerConcurrencyTest {

    private RecordingScheduledExecutor executor;

    /** Explicit stop happens-before the close notification: zero reconnects. */
    @Test
    void disconnectBeforeCloseSchedulesNoReconnect() {
        setup(false, false);

        listener.requestReconnect();
        listener.requestDisconnect();
        listener.operationComplete(closeFuture());

        assertThat(executor.scheduledCount()).as("reconnect tasks scheduled").isZero();
    }

    /** Close notification happens-before explicit stop: exactly one reconnect. */
    @Test
    void closeBeforeDisconnectSchedulesExactlyOneReconnect() {
        setup(false, false);

        listener.requestReconnect();
        listener.operationComplete(closeFuture());
        listener.requestDisconnect();

        assertThat(executor.scheduledCount()).as("reconnect tasks scheduled").isOne();
    }

    /**
     * Interleaving A - close wins: the close handler runs to completion before
     * the stop thread sets {@code disconnectRequested}. Exactly one reconnect
     * is scheduled; the late stop cannot revoke it.
     */
    @Test
    void closeCompletesBeforeStopSchedulesOneReconnect() throws InterruptedException {
        setup(false, false);
        listener.requestReconnect();

        final Thread closer = startCloseThread();
        closer.join();

        final Thread stopper = new Thread(listener::requestDisconnect, "disconnect-thread");
        stopper.start();
        stopper.join();

        assertThat(executor.scheduledCount()).isOne();
    }

    /**
     * Interleaving B - stop wins the check: the close handler is parked at the
     * very start of {@code operationComplete}; stop sets the flag first, so the
     * {@code scheduleReconnect} check at ReconnectOnCloseListener.kt:36
     * suppresses the reconnect and {@code schedule()} is never reached.
     */
    @Test
    void stopSetWhileCloseHandlerParkedSchedulesNoReconnect() throws InterruptedException {
        setup(true, false);
        listener.requestReconnect();

        final Thread closer = startCloseThread();
        listener.enterGate.await();

        final Thread stopper = new Thread(listener::requestDisconnect, "disconnect-thread");
        stopper.start();
        stopper.join();

        listener.allowEnter.countDown();
        closer.join();

        assertThat(executor.enteredSchedule())
                .as("schedule() must not be called when stop wins")
                .isFalse();
        assertThat(executor.scheduledCount()).isZero();
    }

    /**
     * Interleaving C - stop lands after the check: the close handler is parked
     * inside {@code schedule()} (the line-36 check already passed). The stop
     * flips the flag but the single task is still recorded exactly once.
     */
    @Test
    void stopAfterCheckPassedStillSchedulesAtMostOne() throws InterruptedException {
        setup(false, true);
        listener.requestReconnect();

        final Thread closer = startCloseThread();
        executor.arriveAtScheduleGate.await();

        final Thread stopper = new Thread(listener::requestDisconnect, "disconnect-thread");
        stopper.start();
        stopper.join();

        executor.allowScheduling.countDown();
        closer.join();

        assertThat(executor.scheduledCount())
                .as("a late stop neither removes nor duplicates the single task")
                .isOne();
    }

    /**
     * Free-form race, one close event vs. a concurrent stop, released from a
     * common gate 500 times: every iteration must schedule 0 or 1 reconnects,
     * never more than one for the single close event.
     */
    @Test
    void freeForAllRaceNeverSchedulesMoreThanOneReconnect() throws InterruptedException {
        final int iterations = 500;
        for (int i = 0; i < iterations; i++) {
            setup(false, false);
            listener.requestReconnect();

            final var start = new CountDownLatch(1);
            final var done = new CountDownLatch(2);
            final var error = new AtomicReference<Throwable>();

            final Thread closer =
                    new Thread(
                            () -> {
                                try {
                                    start.await();
                                    listener.operationComplete(closeFuture());
                                } catch (Throwable t) {
                                    error.compareAndSet(t, t);
                                } finally {
                                    done.countDown();
                                }
                            },
                            "close-thread-" + i);
            final Thread stopper =
                    new Thread(
                            () -> {
                                try {
                                    start.await();
                                    listener.requestDisconnect();
                                } catch (Throwable t) {
                                    error.compareAndSet(t, t);
                                } finally {
                                    done.countDown();
                                }
                            },
                            "disconnect-thread-" + i);

            closer.start();
            stopper.start();
            start.countDown();
            done.await();

            assertThat(error.get()).isNull();
            assertThat(executor.scheduledCount())
                    .as("iteration %s: at most one reconnect task", i)
                    .isBetween(0, 1);
        }
    }

    private GatedCloseListener listener;

    private Thread startCloseThread() {
        final Thread closer =
                new Thread(() -> listener.operationComplete(closeFuture()), "close-thread");
        closer.start();
        return closer;
    }

    private void setup(boolean gateEnter, boolean gateSchedule) {
        executor = new RecordingScheduledExecutor(gateSchedule);

        @SuppressWarnings("unchecked")
        final Iso8583Client<?> client = mock(Iso8583Client.class);
        listener = new GatedCloseListener(client, 100, executor, gateEnter);
    }

    private static ChannelFuture closeFuture() {
        final Channel channel = mock(Channel.class);
        final ChannelFuture future = mock(ChannelFuture.class);
        when(future.channel()).thenReturn(channel);
        lenient().when(channel.disconnect()).thenReturn(future);
        return future;
    }

    /**
     * Adds an optional latch at the entry of {@code operationComplete} so a
     * test can park the event-loop thread exactly there.
     */
    private static final class GatedCloseListener extends ReconnectOnCloseListener {

        private final CountDownLatch enterGate;
        private final CountDownLatch allowEnter;
        private final boolean gated;

        GatedCloseListener(
                Iso8583Client<?> client,
                int reconnectInterval,
                ScheduledExecutorService executorService,
                boolean gated
        ) {
            super(client, reconnectInterval, executorService);
            this.gated = gated;
            this.enterGate = gated ? new CountDownLatch(1) : new CountDownLatch(0);
            this.allowEnter = gated ? new CountDownLatch(1) : new CountDownLatch(0);
        }

        @Override
        public void operationComplete(ChannelFuture future) {
            if (gated) {
                enterGate.countDown();
                try {
                    allowEnter.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            super.operationComplete(future);
        }
    }

    /**
     * Records {@code schedule(Callable)} calls without running them. With
     * gating enabled the calling thread is parked inside {@code schedule()}
     * until released, modelling the exact point after the
     * {@code disconnectRequested} check. No worker threads are created.
     */
    private static final class RecordingScheduledExecutor
        extends AbstractExecutorService
        implements ScheduledExecutorService {

        private final AtomicInteger scheduled = new AtomicInteger();
        private final AtomicInteger enteredSchedule = new AtomicInteger();
        private final List<ScheduledFuture<?>> tasks = new CopyOnWriteArrayList<>();
        private final CountDownLatch arriveAtScheduleGate;
        private final CountDownLatch allowScheduling;

        RecordingScheduledExecutor(boolean gated) {
            this.arriveAtScheduleGate = gated ? new CountDownLatch(1) : new CountDownLatch(0);
            this.allowScheduling = gated ? new CountDownLatch(1) : new CountDownLatch(0);
        }

        int scheduledCount() {
            return scheduled.get();
        }

        boolean enteredSchedule() {
            return enteredSchedule.get() > 0;
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> ScheduledFuture<V> schedule(
                Callable<V> callable,
                long delay,
                TimeUnit unit
        ) {
            enteredSchedule.incrementAndGet();
            arriveAtScheduleGate.countDown();
            try {
                allowScheduling.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            scheduled.incrementAndGet();
            final ScheduledFuture<V> task = mock(ScheduledFuture.class);
            tasks.add(task);
            return task;
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(
                Runnable command,
                long initialDelay,
                long period,
                TimeUnit unit
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command,
                long initialDelay,
                long delay,
                TimeUnit unit
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(Runnable command) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {
            // no resources held
        }

        @Override
        public List<Runnable> shutdownNow() {
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }
    }
}
