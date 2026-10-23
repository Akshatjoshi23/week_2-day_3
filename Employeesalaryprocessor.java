import java.util.concurrent.*;
import java.util.*;

// task for each employee
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

        Thread.sleep(500); // pretend some heavy work

        if (empSalary < 0) {
            throw new IllegalArgumentException("Invalid salary for " + empName);
        }

        double bonus = empSalary * 0.10;
        double totalPay = empSalary + bonus;

        return "ID: " + id + "\nName: " + empName + "\nSalary: " + empSalary +
               "\nBonus (10%): " + bonus + "\nTotal Pay: " + totalPay;
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

            // submit all
            for (SalaryTask t : myTasks) {
                Future<String> f = pool.submit(t);
                allResults.add(f);
            }

            System.out.println("\n--- Employee Salary Reports ---");

            for (Future<String> f : allResults) {
                try {
                    String report = f.get(); // get result
                    System.out.println("\n" + report);
                } catch (ExecutionException e) {
                    System.out.println("Failed for employee: " + e.getCause().getMessage());
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Main thread got interrupted");

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

        System.out.println("\nAll employees processed");
    }
}