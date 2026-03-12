import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * InventoryManager
 *
 * - Tracks product stock levels in real-time (O(1) checks)
 * - Processes purchase requests in O(1) average time using atomic operations
 * - Prevents overselling via CAS on AtomicInteger
 * - Maintains FIFO waiting list per product using ConcurrentLinkedQueue
 * - Provides restock method that fulfills waiting list entries
 *
 * Demo main simulates 50,000 purchase attempts against a product with 100 units.
 */
public class InventoryManager {

    // productId -> available stock (atomic)
    private final ConcurrentHashMap<String, AtomicInteger> stockMap = new ConcurrentHashMap<>();

    // productId -> waiting list (FIFO) of userIds
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> waitlists = new ConcurrentHashMap<>();

    // Optional: track attempts and successful purchases for metrics
    private final ConcurrentHashMap<String, AtomicLong> attemptCounter = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> successCounter = new ConcurrentHashMap<>();

    // Create product with initial stock
    public void createProduct(String productId, int initialStock) {
        stockMap.put(productId, new AtomicInteger(Math.max(0, initialStock)));
        waitlists.putIfAbsent(productId, new ConcurrentLinkedQueue<>());
        attemptCounter.putIfAbsent(productId, new AtomicLong(0));
        successCounter.putIfAbsent(productId, new AtomicLong(0));
    }

    // O(1) stock check
    public int checkStock(String productId) {
        AtomicInteger stock = stockMap.get(productId);
        return stock == null ? 0 : stock.get();
    }

    /**
     * Attempt to purchase one unit.
     *
     * Returns a PurchaseResult describing:
     * - success: true if purchase succeeded and stock decremented
     * - remainingStock: remaining units after successful purchase (undefined if added to waitlist)
     * - waitlistPosition: if added to waitlist, the 1-based position; otherwise -1
     */
    public PurchaseResult purchaseItem(String productId, String userId) {
        attemptCounter.computeIfAbsent(productId, k -> new AtomicLong(0)).incrementAndGet();

        AtomicInteger stock = stockMap.get(productId);
        if (stock == null) {
            // product not found
            return new PurchaseResult(false, 0, -1, "PRODUCT_NOT_FOUND");
        }

        // Optimistic CAS loop to decrement stock atomically and avoid oversell
        while (true) {
            int current = stock.get();
            if (current <= 0) {
                // No stock: add to waiting list (FIFO)
                ConcurrentLinkedQueue<String> queue = waitlists.computeIfAbsent(productId, k -> new ConcurrentLinkedQueue<>());
                queue.add(userId);
                int position = queue.size(); // approximate position; size() is O(n) for ConcurrentLinkedQueue but acceptable for small queues; if strict O(1) needed, maintain AtomicInteger per queue
                return new PurchaseResult(false, 0, position, "ADDED_TO_WAITLIST");
            }
            // try to decrement
            boolean updated = stock.compareAndSet(current, current - 1);
            if (updated) {
                successCounter.computeIfAbsent(productId, k -> new AtomicLong(0)).incrementAndGet();
                return new PurchaseResult(true, current - 1, -1, "SUCCESS");
            }
            // else retry
        }
    }

    /**
     * Restock product by 'amount' units and fulfill waiting list in FIFO order.
     * Returns number of waiting-list entries fulfilled.
     */
    public int restockAndFulfill(String productId, int amount) {
        if (amount <= 0) return 0;
        AtomicInteger stock = stockMap.get(productId);
        if (stock == null) return 0;

        // Atomically add stock
        int newStock = stock.addAndGet(amount);

        // Fulfill waiting list while stock > 0
        ConcurrentLinkedQueue<String> queue = waitlists.computeIfAbsent(productId, k -> new ConcurrentLinkedQueue<>());
        int fulfilled = 0;
        while (newStock > 0) {
            String nextUser = queue.poll();
            if (nextUser == null) break;
            // allocate one unit to nextUser: decrement stock by 1
            // We already added 'amount' to stock; now consume one per fulfilled user
            int before, after;
            do {
                before = stock.get();
                if (before <= 0) break; // should not happen
                after = before - 1;
            } while (!stock.compareAndSet(before, after));
            fulfilled++;
            newStock = stock.get();
            // In a real system, notify nextUser (email/push) and create an order/reservation
        }
        return fulfilled;
    }

    // Cancel a waiting-list entry (user gives up); returns true if removed
    public boolean cancelWaiting(String productId, String userId) {
        ConcurrentLinkedQueue<String> queue = waitlists.get(productId);
        if (queue == null) return false;
        return queue.remove(userId);
    }

    // Get waiting list size (position info)
    public int getWaitingListSize(String productId) {
        ConcurrentLinkedQueue<String> queue = waitlists.get(productId);
        return queue == null ? 0 : queue.size();
    }

    // Get metrics
    public long getAttemptCount(String productId) {
        AtomicLong v = attemptCounter.get(productId);
        return v == null ? 0L : v.get();
    }

    public long getSuccessCount(String productId) {
        AtomicLong v = successCounter.get(productId);
        return v == null ? 0L : v.get();
    }

    // Purchase result DTO
    public static class PurchaseResult {
        public final boolean success;
        public final int remainingStock;
        public final int waitlistPosition; // -1 if not on waitlist
        public final String code; // e.g., SUCCESS, ADDED_TO_WAITLIST, PRODUCT_NOT_FOUND

        public PurchaseResult(boolean success, int remainingStock, int waitlistPosition, String code) {
            this.success = success;
            this.remainingStock = remainingStock;
            this.waitlistPosition = waitlistPosition;
            this.code = code;
        }

        @Override
        public String toString() {
            if (success) {
                return "Success, " + remainingStock + " units remaining";
            } else if ("ADDED_TO_WAITLIST".equals(code)) {
                return "Added to waiting list, position #" + waitlistPosition;
            } else if ("PRODUCT_NOT_FOUND".equals(code)) {
                return "Product not found";
            } else {
                return "Failed: " + code;
            }
        }
    }

    // -------------------------
    // Demo / Benchmark
    // -------------------------
    public static void main(String[] args) throws InterruptedException {
        final String PRODUCT = "IPHONE15_256GB";
        final int initialStock = 100;
        final int concurrentAttempts = 50_000;
        final int poolSize = 500; // thread pool size for executing tasks

        InventoryManager manager = new InventoryManager();
        manager.createProduct(PRODUCT, initialStock);

        ExecutorService exec = Executors.newFixedThreadPool(poolSize);
        CountDownLatch readyLatch = new CountDownLatch(concurrentAttempts);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(concurrentAttempts);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger waitlistCount = new AtomicInteger(0);

        long t0 = System.nanoTime();

        for (int i = 0; i < concurrentAttempts; i++) {
            final String userId = "user-" + i;
            exec.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await(); // ensure near-simultaneous start
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                PurchaseResult res = manager.purchaseItem(PRODUCT, userId);
                if (res.success) successCount.incrementAndGet();
                else if ("ADDED_TO_WAITLIST".equals(res.code)) waitlistCount.incrementAndGet();
                doneLatch.countDown();
            });
        }

        // Wait until all tasks are ready, then start them together
        readyLatch.await();
        startLatch.countDown();
        doneLatch.await();

        long t1 = System.nanoTime();
        double seconds = (t1 - t0) / 1_000_000_000.0;

        System.out.println("=== Flash Sale Simulation ===");
        System.out.println("Product: " + PRODUCT);
        System.out.println("Initial stock: " + initialStock);
        System.out.println("Concurrent attempts: " + concurrentAttempts);
        System.out.println("Successful purchases: " + successCount.get());
        System.out.println("Added to waiting list: " + waitlistCount.get());
        System.out.println("Final stock (should be 0): " + manager.checkStock(PRODUCT));
        System.out.println("Waiting list size: " + manager.getWaitingListSize(PRODUCT));
        System.out.printf("Elapsed time: %.3f seconds%n", seconds);

        // Example: restock 10 units and fulfill waiting list
        int restocked = 10;
        int fulfilled = manager.restockAndFulfill(PRODUCT, restocked);
        System.out.println("Restocked " + restocked + " units; fulfilled waiting list entries: " + fulfilled);
        System.out.println("Stock after restock/fulfill: " + manager.checkStock(PRODUCT));
        System.out.println("Waiting list size after fulfill: " + manager.getWaitingListSize(PRODUCT));

        exec.shutdownNow();
    }
}
