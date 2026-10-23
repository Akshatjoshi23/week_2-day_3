import java.util.concurrent.*;
import java.util.*;

class SalaryTask implements Callable<String> {
    private int id;
    private String empName;
    private double empSalary;

    SalaryTask(int id, String name, double salary) {
        this.id = id;
        this.empName = name;
        this.empSalary = salary;
    }

    @Override
    public String call() throws Exception {
        System.out.println("Working on " + empName + " by " + Thread.currentThread().getName());
        Thread.sleep(500);
        if (empSalary < 0) {
            throw new IllegalArgumentException("Invalid salary for " + empName);
        }
        double bonus = empSalary * 0.10;
        return "ID: " + id + " Name: " + empName + " Total: " + (empSalary + bonus);
    }
}

public class EmployeeSalaryProcessor {
    public static void main(String[] args) {

        ExecutorService pool = Executors.newFixedThreadPool(3);
        List<Future<String>> allResults = new ArrayList<>();

        try {
            List<SalaryTask> myTasks = List.of(
                new SalaryTask(101, "Amit", 30000),
                new SalaryTask(102, "Rahul", 40000),
                new SalaryTask(103, "Priya", 50000),
                new SalaryTask(104, "Neha", 35000)
            );

            // submit vs execute + Callable vs Runnable
            Runnable simpleRunnable = () -> System.out.println("Runnable task running: " + Thread.currentThread().getName());
            pool.execute(simpleRunnable);
            Future<?> runnableFuture = pool.submit(simpleRunnable);

            for (SalaryTask t : myTasks) {
                Future<String> f = pool.submit(t);
                allResults.add(f);
            }

            System.out.println("\n--- Reports with Future checks ---");
            for (Future<String> f : allResults) {
                System.out.println("Is done? " + f.isDone());
                try {
                    String report = f.get(2, TimeUnit.SECONDS); // get with timeout
                    System.out.println(report);
                } catch (TimeoutException e) {
                    System.out.println("Task timed out, cancelling");
                    f.cancel(true);
                } catch (ExecutionException e) {
                    System.out.println("Failed: " + e.getCause().getMessage());
                }
            }

            // Future cancel demo
            Future<String> longTask = pool.submit(() -> {
                Thread.sleep(5000);
                return "long task done";
            });
            Thread.sleep(200);
            System.out.println("Cancelling long task: " + longTask.cancel(true));
            System.out.println("Is cancelled: " + longTask.isCancelled());

            // invokeAll
            System.out.println("\n--- invokeAll demo ---");
            List<SalaryTask> batch = List.of(new SalaryTask(201, "A", 1000), new SalaryTask(202, "B", 2000));
            try {
                List<Future<String>> batchResults = pool.invokeAll(batch);
                for (Future<String> fr : batchResults) System.out.println(fr.get());
            } catch (Exception e) { e.printStackTrace(); }

            // invokeAny
            System.out.println("\n--- invokeAny demo ---");
            try {
                String first = pool.invokeAny(batch);
                System.out.println("First finished: " + first);
            } catch (Exception e) { e.printStackTrace(); }

            // CompletionService
            System.out.println("\n--- CompletionService demo ---");
            CompletionService<String> completionService = new ExecutorCompletionService<>(pool);
            for (SalaryTask t : myTasks) completionService.submit(t);
            for (int i = 0; i < myTasks.size(); i++) {
                try {
                    Future<String> done = completionService.take();
                    System.out.println("Completed as it comes: " + done.get());
                } catch (Exception e) { e.printStackTrace(); }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                }
            } catch (InterruptedException e) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Other pools
        ExecutorService cached = Executors.newCachedThreadPool();
        cached.submit(() -> System.out.println("cached pool task"));
        cached.shutdown();

        ExecutorService single = Executors.newSingleThreadExecutor();
        single.submit(() -> System.out.println("single thread pool task"));
        single.shutdown();

        ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(2);
        scheduled.schedule(() -> System.out.println("scheduled task ran after delay"), 1, TimeUnit.SECONDS);
        scheduled.shutdown();

        ExecutorService stealing = Executors.newWorkStealingPool();
        stealing.submit(() -> System.out.println("work stealing pool task"));
        stealing.shutdown();

        // ThreadPoolExecutor core params + RejectedExecutionHandler
        System.out.println("\n--- ThreadPoolExecutor custom demo ---");
        ThreadPoolExecutor customPool = new ThreadPoolExecutor(
                2,
                4,
                10,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        for (int i = 0; i < 6; i++) {
            int num = i;
            customPool.execute(() -> System.out.println("custom pool task " + num));
        }
        customPool.shutdown();

        System.out.println("\nAll executor topics covered");
    }
}Does it covers all topic