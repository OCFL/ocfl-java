package edu.wisc.library.ocfl.itest.s3;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.aws.OcflS3Client;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.cache.NoOpCache;
import edu.wisc.library.ocfl.core.db.ObjectDetailsDatabaseBuilder;
import edu.wisc.library.ocfl.core.extension.layout.config.DefaultLayoutConfig;
import edu.wisc.library.ocfl.core.lock.ObjectLockBuilder;
import edu.wisc.library.ocfl.core.path.constraint.ContentPathConstraints;
import edu.wisc.library.ocfl.core.storage.cloud.CloudOcflStorage;
import edu.wisc.library.ocfl.itest.ITestHelper;
import edu.wisc.library.ocfl.itest.MutableHeadITest;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import static edu.wisc.library.ocfl.itest.ITestHelper.expectedRepoPath;

public class S3MutableHeadITest extends MutableHeadITest {

    @RegisterExtension
    public static S3MockExtension S3_MOCK = S3MockExtension.builder().silent().build();

    private final S3Client s3Client = S3_MOCK.createS3ClientV2();

    private S3ITestHelper s3Helper;
    private ComboPooledDataSource dataSource;
    private Set<String> createdBuckets = new HashSet<>();

    @Override
    protected void onBefore() {
        s3Helper = new S3ITestHelper(s3Client);
        dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl("jdbc:h2:mem:test");
    }

    @Override
    protected void onAfter() {
        truncateObjectDetails(dataSource);
        deleteBuckets();
    }

    @Override
    protected MutableOcflRepository defaultRepo(String name) {
        createBucket(name);
        var repo = new OcflRepositoryBuilder()
                .layoutConfig(DefaultLayoutConfig.flatUrlConfig())
                .inventoryCache(new NoOpCache<>())
                .objectLock(new ObjectLockBuilder().buildDbLock(dataSource))
                .objectDetailsDb(new ObjectDetailsDatabaseBuilder().build(dataSource))
                .inventoryMapper(ITestHelper.testInventoryMapper())
                .contentPathConstraintProcessor(ContentPathConstraints.cloud())
                .storage(CloudOcflStorage.builder()
                        .objectMapper(ITestHelper.prettyPrintMapper())
                        .cloudClient(new OcflS3Client(s3Client, name))
                        .workDir(workDir)
                        .build())
                .workDir(workDir)
                .buildMutable();
        ITestHelper.fixTime(repo, "2019-08-05T15:57:53.703314Z");
        return repo;
    }

    @Override
    protected void verifyRepo(String name) {
        s3Helper.verifyRepo(expectedRepoPath(name), name);
    }

    @Override
    protected void writeFile(String repoName, String path, InputStream content) {
        try {
            s3Client.putObject(PutObjectRequest.builder()
                    .key(path)
                    .bucket(repoName)
                    .build(), RequestBody.fromBytes(content.readAllBytes()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void truncateObjectDetails(DataSource dataSource) {
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement("TRUNCATE TABLE ocfl_object_details")) {
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createBucket(String name) {
        s3Client.createBucket(CreateBucketRequest.builder().bucket(name).build());
        createdBuckets.add(name);
    }

    private void deleteBuckets() {
        createdBuckets.forEach(bucket -> {
            s3Helper.deleteBucket(bucket);
        });
    }

}
