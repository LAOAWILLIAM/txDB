package txDB;

import test.buffer.BufferManagerTest;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class TxDB {
    public static void main(String[] args) {
        System.out.println("Welcome to txDB");

        Result result = JUnitCore.runClasses(BufferManagerTest.class);

        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
        }

        System.out.println(result.wasSuccessful());
    }
}

