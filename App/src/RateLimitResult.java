import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

public class RateLimitResult {

    public static class RateLimitResult {
        public final boolean allowed;
        public final long remainingTokens;
        public final long retryAfterSeconds;
        public final String message;

        public RateLimitResult(boolean allowed, long remainingTokens, long retryAfterSeconds, String message) {
            this.allowed = allowed;
            this.remainingTokens = remainingTokens;
            this.retryAfterSeconds = retryAfterSeconds;
            this.message = message;
        }

        @Override
        public String toString() {
            if (allowed) {
                return String.format("Allowed (%d requests remaining)", remainingTokens);
            } else {
                return String.format("Denied (0 remaining, retry after %ds) - %s", retryAfterSeconds, message);
            }
        }
    }

    public static class RateLimitStatus {
        public final long used;
        public final long limit;
        public final long resetEpochSeconds;

        public RateLimitStatus(long used, long limit, long resetEpochSeconds) {
            this.used = used;
            this.limit = limit;
            this.resetEpochSeconds = resetEpochSeconds;
        }

        @Override
        public String toString() {
            return String.format("{used: %d, limit: %d, reset: %d}", used, limit, resetEpochSeconds);
        }
    }

    public static class TokenBucket {
        private final long maxTokens;
        private final double refillPerMillis;
        private double tokens;
        private long lastRefillTs;
        private final Object lock = new Object();

        public TokenBucket(long maxTokens, long refillWindowMillis) {
            this.maxTokens = maxTokens;
            this.refillPerMillis = (double) maxTokens / (double) Math.max(1, refillWindowMillis);
            this.tokens = maxTokens;
            this.lastRefillTs = System.currentTimeMillis();
        }

        public RateLimitResult tryConsume() {
            synchronized (lock) {
                refill();
                if (tokens >= 1.0) {
                    tokens -= 1.0;
                    long remaining = (long) Math.floor(tokens);
                    return new RateLimitResult(true, remaining, 0, "OK");
                } else {
                    double needed = 1.0 - tokens;
                    long millis = (long) Math.ceil(needed / refillPerMillis);
                    long retrySec = Math.max(1, millis / 1000);
                    return new RateLimitResult(false, 0, retrySec, "Rate limit exceeded");
                }
            }
        }

        public RateLimitStatus getStatus() {
            synchronized (lock) {
                refill();
                long used = maxTokens - (long) Math.floor(tokens);
                long resetInMillis = (long) Math.ceil((maxTokens - tokens) / refillPerMillis);
                long resetEpoch = Instant.now().getEpochSecond() + Math.max(0, resetInMillis / 1000);
                return new RateLimitStatus(used, maxTokens, resetEpoch);
            }
        }

        private void refill() {
            long now = System.currentTimeMillis();
            if (now <= lastRefillTs) return;
            long elapsed = now - lastRefillTs;
            double add = elapsed * refillPerMillis;
            if (add > 0) {
                tokens = Math.min((double) maxTokens, tokens + add);
                lastRefillTs = now;
            }
        }
    }

    public static class InMemoryRateLimiter {
        private final ConcurrentHashMap<String, TokenBucket> buckets = new ConcurrentHashMap<>();
        private final long maxTokens;
        private final long refillWindowMillis;

        public InMemoryRateLimiter(long maxTokens, long refillWindowMillis) {
            this.maxTokens = maxTokens;
            this.refillWindowMillis = refillWindowMillis;
        }

        public RateLimitResult checkRateLimit(String clientId) {
            TokenBucket tb = buckets.computeIfAbsent(clientId, k -> new TokenBucket(maxTokens, refillWindowMillis));
            return tb.tryConsume();
        }

        public RateLimitStatus getRateLimitStatus(String clientId) {
            TokenBucket tb = buckets.computeIfAbsent(clientId, k -> new TokenBucket(maxTokens, refillWindowMillis));
            return tb.getStatus();
        }
    }

    public static class RedisRateLimiter {
        private final JedisPool jedisPool;
        private final long maxTokens;
        private final long refillWindowMillis;
        private final String luaScript;
        private final String scriptSha1;

        public RedisRateLimiter(String redisHost, int redisPort, long maxTokens, long refillWindowMillis) {
            this.jedisPool = new JedisPool(redisHost, redisPort);
            this.maxTokens = maxTokens;
            this.refillWindowMillis = refillWindowMillis;

            this.luaScript =
                    "local key = KEYS[1]\n" +
                            "local now = tonumber(ARGV[1])\n" +
                            "local refillPerMs = tonumber(ARGV[2])\n" +
                            "local maxTokens = tonumber(ARGV[3])\n" +
                            "local consume = tonumber(ARGV[4])\n" +
                            "local data = redis.call('HMGET', key, 'tokens', 'lastRefill')\n" +
                            "local tokens = tonumber(data[1]) or maxTokens\n" +
                            "local last = tonumber(data[2]) or now\n" +
                            "if now > last then\n" +
                            "  local added = (now - last) * refillPerMs\n" +
                            "  tokens = math.min(maxTokens, tokens + added)\n" +
                            "end\n" +
                            "if tokens >= consume then\n" +
                            "  tokens = tokens - consume\n" +
                            "  redis.call('HMSET', key, 'tokens', tostring(tokens), 'lastRefill', tostring(now))\n" +
                            "  redis.call('PEXPIRE', key, 3600000)\n" +
                            "  return {1, tostring(math.floor(tokens)), '0'}\n" +
                            "else\n" +
                            "  local needed = consume - tokens\n" +
                            "  local retryMs = math.ceil(needed / refillPerMs)\n" +
                            "  return {0, '0', tostring(retryMs)}\n" +
                            "end";

            try (Jedis j = jedisPool.getResource()) {
                this.scriptSha1 = j.scriptLoad(luaScript);
            }
        }

        private String redisKey(String clientId) {
            return "rl:" + clientId;
        }

        public RateLimitResult checkRateLimit(String clientId) {
            long now = System.currentTimeMillis();
            double refillPerMs = (double) maxTokens / (double) Math.max(1, refillWindowMillis);
            try (Jedis j = jedisPool.getResource()) {
                Object res = j.evalsha(scriptSha1, 1, redisKey(clientId),
                        String.valueOf(now),
                        String.valueOf(refillPerMs),
                        String.valueOf(maxTokens),
                        String.valueOf(1));
                if (res instanceof java.util.List) {
                    @SuppressWarnings("unchecked")
                    java.util.List<Object> list = (java.util.List<Object>) res;
                    long allowed = Long.parseLong(list.get(0).toString());
                    long remaining = Long.parseLong(list.get(1).toString());
                    long retryMs = Long.parseLong(list.get(2).toString());
                    if (allowed == 1) {
                        return new RateLimitResult(true, remaining, 0, "OK");
                    } else {
                        long retrySec = Math.max(1, (int) Math.ceil(retryMs / 1000.0));
                        return new RateLimitResult(false, 0, retrySec, "Rate limit exceeded");
                    }
                } else {
                    return new RateLimitResult(false, 0, 1, "Redis script error");
                }
            }
        }

        public RateLimitStatus getRateLimitStatus(String clientId) {
            try (Jedis j = jedisPool.getResource()) {
                String key = redisKey(clientId);
                java.util.List<String> vals = j.hmget(key, "tokens", "lastRefill");
                String tokensStr = vals.get(0);
                String lastStr = vals.get(1);
                double tokens = tokensStr == null ? maxTokens : Double.parseDouble(tokensStr);
                long last = lastStr == null ? System.currentTimeMillis() : Long.parseLong(lastStr);
                long used = maxTokens - (long) Math.floor(tokens);
                double refillPerMs = (double) maxTokens / (double) Math.max(1, refillWindowMillis);
                long millisToFull = (long) Math.ceil((maxTokens - tokens) / refillPerMs);
                long resetEpoch = Instant.now().getEpochSecond() + Math.max(0, millisToFull / 1000);
                return new RateLimitStatus(used, maxTokens, resetEpoch);
            }
        }

        public void close() {
            jedisPool.close();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        final long LIMIT = 1000;
        final long HOUR_MS = 3600_000L;

        System.out.println("=== In-memory Rate Limiter Demo ===");
        InMemoryRateLimiter memLimiter = new InMemoryRateLimiter(LIMIT, HOUR_MS);
        String client = "abc123";

        System.out.println(memLimiter.checkRateLimit(client));
        System.out.println(memLimiter.checkRateLimit(client));
        System.out.println(memLimiter.checkRateLimit(client));
        System.out.println("Status: " + memLimiter.getRateLimitStatus(client));

        System.out.println("\n=== Redis Rate Limiter Demo (requires Redis) ===");
        String redisHost = "127.0.0.1";
        int redisPort = 6379;
        RedisRateLimiter redisLimiter = null;
        try {
            redisLimiter = new RedisRateLimiter(redisHost, redisPort, LIMIT, HOUR_MS);
            System.out.println(redisLimiter.checkRateLimit(client));
            System.out.println(redisLimiter.checkRateLimit(client));
            System.out.println(redisLimiter.checkRateLimit(client));
            System.out.println("Status: " + redisLimiter.getRateLimitStatus(client));
        } catch (Exception ex) {
            System.out.println("Redis demo skipped (could not connect): " + ex.getMessage());
        } finally {
            if (redisLimiter != null) redisLimiter.close();
        }
    }
}