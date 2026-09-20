package com.github.kpavlov.jreactive8583.netty.pipeline;

import com.github.kpavlov.jreactive8583.client.Iso8583Client;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Proves the terminal-state invariant behind reconnect scheduling:
 * <p>
 * "When a stop/disconnect and a channel close happen concurrently for one
 * connection episode, at most one reconnect is scheduled."
 * <p>
 * The test uses a single-thread recording executor and a {@link CyclicBarrier}
 * to explore both interleavings deterministically - no sleeps, no sockets,
 * no event loop threads, no listening ports.
 */
class ReconnectOnCloseListenerConcurrencyTest {

    /**
     * Schedules are recorded but never run, so the production callback
     * ({@code client.connectAsync()}) is never actually invoked.
     */
    private static final class RecordingScheduledExecutor
            extends ScheduledThreadPoolExecutor {

        private final AtomicInteger scheduleCount = new AtomicInteger();

        RecordingScheduledExecutor() {
            super(1);
            setRemoveOnCancelPolicy(true);
        }

        @Override
        public ScheduledFuture<?> schedule(
                final Runnable command,
                final long delay,
                final TimeUnit unit) {
            scheduleCount.incrementAndGet();
            return super.schedule(() -> { }, delay, unit);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(
                final Callable<V> callable,
                final long delay,
                final TimeUnit unit) {
            scheduleCount.incrementAndGet();
            // Replace the production task with a no-op so connectAsync()
            // is never called by the recording executor.
            return super.schedule(() -> null, delay, unit);
        }
    }

    private RecordingScheduledExecutor executor;

    private ReconnectOnCloseListener newListener() {
        executor = new RecordingScheduledExecutor();
        final Iso8583Client<?> client = mock(Iso8583Client.class);
        when(client.connectAsync()).thenReturn(null);
        return new ReconnectOnCloseListener(client, 1_000_000, executor);
    }

    private void dispose(final EmbeddedChannel channel) throws InterruptedException {
        // Drops all inbound/outbound data and closes the channel,
        // leaving no channel registered in any event loop (EmbeddedChannel
        // uses the calling thread; no Netty I/O threads are created here).
        channel.finishAndReleaseAll();
        // shutdownNow discards every recorded but un-run reconnect task,
        // guaranteeing no queued task or worker thread survives the test.
        executor.shutdownNow();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS))
                .as("recording executor must terminate")
                .isTrue();
    }

    @RepeatedTest(200)
    void schedulesAtMostOneReconnectWhenStopAndCloseRace() throws Exception {
        final var listener = newListener();
        final var channel = new EmbeddedChannel();
        channel.closeFuture().addListener(listener);

        final var start = new CyclicBarrier(2);
        final var done = new CyclicBarrier(2);
        final AtomicInteger stopCount = new AtomicInteger();
        final AtomicInteger closeCount = new AtomicInteger();

        final Thread stopper = new Thread(() -> {
            try {
                start.await();
                listener.requestDisconnect();
                stopCount.incrementAndGet();
                done.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        }, "test-stop");

        final Thread closer = new Thread(() -> {
            try {
                start.await();
                // closeFuture notifies ReconnectOnCloseListener.operationComplete exactly once
                channel.close();
                closeCount.incrementAndGet();
                done.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        }, "test-close");

        stopper.start();
        closer.start();
        stopper.join();
        closer.join();

        assertThat(channel.isActive()).isFalse();
        assertThat(stopCount.get()).isEqualTo(1);
        assertThat(closeCount.get()).isEqualTo(1);
        assertThat(executor.scheduleCount.get())
                .as("at most one reconnect may be scheduled per stop/close episode")
                .isBetween(0, 1);

        dispose(channel);
    }

    @Test
    void remoteCloseWithoutStopSchedulesExactlyOneReconnect() throws Exception {
        final var listener = newListener();
        final var channel = new EmbeddedChannel();
        channel.closeFuture().addListener(listener);

        channel.close();

        assertThat(channel.isActive()).isFalse();
        assertThat(executor.scheduleCount.get())
                .as("remote close must request exactly one reconnect")
                .isEqualTo(1);

        dispose(channel);
    }

    @Test
    void stopBeforeCloseSchedulesNoReconnect() throws Exception {
        final var listener = newListener();
        final var channel = new EmbeddedChannel();
        channel.closeFuture().addListener(listener);

        listener.requestDisconnect();
        channel.close();

        assertThat(executor.scheduleCount.get())
                .as("explicit stop before close must suppress reconnect")
                .isZero();

        dispose(channel);
    }
}
