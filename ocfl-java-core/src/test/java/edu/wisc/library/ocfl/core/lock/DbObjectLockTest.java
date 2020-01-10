package edu.wisc.library.ocfl.core.lock;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import edu.wisc.library.ocfl.api.exception.LockException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class DbObjectLockTest {

    private ObjectLock lock;
    private ComboPooledDataSource dataSource;
    private ExecutorService executor;

    @BeforeEach
    public void setup() {
        dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl("jdbc:h2:mem:test");
//        dataSource.setJdbcUrl( "jdbc:postgresql://localhost/ocfl" );
//        dataSource.setUser("pwinckles");
//        dataSource.setPassword("");

        lock = new ObjectLockBuilder().waitTime(500, TimeUnit.MILLISECONDS).buildDbLock(dataSource);
        executor = Executors.newFixedThreadPool(2);

        truncateObjectLock(dataSource);
    }

    @Test
    public void shouldAcquireLockWhenDoesNotExist() {
        var result = new AtomicBoolean(false);
        lock.doInWriteLock("obj1", () -> {
            result.set(true);
        });
        assertTrue(result.get());
    }

    @Test
    public void shouldAcquireLockWhenAlreadyExistsButNotHeld() {
        var result = new AtomicBoolean(false);
        lock.doInWriteLock("obj1", () -> {
            result.set(true);
        });
        assertTrue(result.get());

        lock.doInWriteLock("obj1", () -> {
            result.set(false);
        });
        assertFalse(result.get());
    }

    @Test
    public void shouldThrowExceptionWhenCannotAcquireLock() throws ExecutionException, InterruptedException {
        var future = executor.submit(() -> {
            lock.doInWriteLock("obj1", () -> {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(3));
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            });
        });

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        var result = new AtomicBoolean(false);
        assertThrows(LockException.class, () -> {
            lock.doInWriteLock("obj1", () -> {
                result.set(true);
            });
        });

        assertFalse(result.get());
        future.get();
    }

    private void truncateObjectLock(DataSource dataSource) {
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement("TRUNCATE TABLE ocfl_object_lock")) {
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
