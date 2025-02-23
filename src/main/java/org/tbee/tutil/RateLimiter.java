package org.tbee.tutil;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.TRACE;

/// Release at most N tokens in a given time frame T.
/// This implementation does not spread the load, like the leaky bucket implementation does, but only limits the number of tokens within the time frame.
/// So all tokens can be claimed at the start, but then the N+1 token will be released when T has passed since the first token was claimed.
/// And N+2 token after T has passed since the 2nd token was claimed.
/// This can result in bursts, but usually the algorithm using this RateLimiter will cause some spreading.
///
/// Example:
///     RateLimiter rateLimiter = new RateLimiter(10, Duration.ofMinutes(1));
///     while (true) {
///         rateLimiter.claim();
///     }
public class RateLimiter {
    private static final System.Logger LOG = System.getLogger(RateLimiter.class.getName());

    private final String name;
    private final Duration timeframe;
    private final DelayQueue<RateLimiterToken> delayQueue;

    /// @param size
    /// @param timeframe
    public RateLimiter(String name, int size, Duration timeframe) {
        this.name = name;
        this.timeframe = timeframe;
        this.delayQueue = new DelayQueue<>();

        // Populate the initial size.
        LocalDateTime now = LocalDateTime.now();
        for (int i = 0; i < size; i++) {
            delayQueue.add(new RateLimiterToken(now));
            LOG.log(TRACE, "{0}: Populating with token available at {1}", name, now);
        }
        LOG.log(DEBUG, "{0}: Populated with {1} tokens", name, delayQueue.size());
    }

    public void claim() {
        claim(null);
    }

    public void claim(String reason) {
        String reasonLog = reason == null ? "" : " for '" + reason + "'";
        try {
            // Obtain token for this session
            LOG.log(TRACE, "{0}: Claiming token{1}, first available is {2}", name, reasonLog, delayQueue.peek().earliestReleaseTime);
            RateLimiterToken token = delayQueue.take();
            LOG.log(DEBUG, "{0}: Claimed token{1} available at {2}", name, reasonLog, token.earliestReleaseTime);

            // Create new token for future
            LocalDateTime earliestReleaseTime = LocalDateTime.now().plus(timeframe);
            delayQueue.add(new RateLimiterToken(earliestReleaseTime));
            LOG.log(TRACE, "{0}: Refilled with token available at {1}", name, earliestReleaseTime);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public List<RateLimiterToken> reduceTo(int size) {
        List<RateLimiterToken> removed = new ArrayList<>();
        while (delayQueue.size() > size) {
            RateLimiterToken[] tokens = delayQueue.toArray(new RateLimiterToken[0]);
            if (tokens.length > 1) {
                RateLimiterToken token = tokens[tokens.length - 1];
                LOG.log(DEBUG, "{0}: Reducing to {1}, currently at {2}, so removing token available at {3}", name, size, tokens.length, token.earliestReleaseTime);
                if (delayQueue.remove(token)) {
                    removed.add(token);
                }
            }
        }
        return removed;
    }

    public void add(List<RateLimiterToken> tokens) {
        tokens.forEach(t -> delayQueue.add(t));
        LOG.log(DEBUG, "{0}: Added {1} tokens, new size = {2}", name, tokens.size(), delayQueue.size());
    }

    public static class RateLimiterToken implements Delayed {
        private final LocalDateTime earliestReleaseTime;

        RateLimiterToken(LocalDateTime earliestReleaseTime) {
            this.earliestReleaseTime = earliestReleaseTime;
        }

        @Override
        public int compareTo(Delayed o) {
            return earliestReleaseTime.compareTo(((RateLimiterToken)o).earliestReleaseTime);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            LocalDateTime now = LocalDateTime.now();
            if (earliestReleaseTime.isBefore(now)) {
                return 0;
            }
            Duration duration = Duration.between(now, earliestReleaseTime);
            return unit.convert(duration);
        }
    }
}
