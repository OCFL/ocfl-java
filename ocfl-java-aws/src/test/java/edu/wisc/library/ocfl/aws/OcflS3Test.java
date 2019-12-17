package edu.wisc.library.ocfl.aws;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.User;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.layout.config.DefaultLayoutConfig;
import edu.wisc.library.ocfl.core.path.constraint.DefaultContentPathConstraints;
import edu.wisc.library.ocfl.core.storage.cloud.CloudOcflStorage;
import edu.wisc.library.ocfl.core.util.FileUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(S3MockExtension.class)
public class OcflS3Test {

    @TempDir
    public Path tempDir;

    @Test
    public void basicMutableHeadTest(S3Client s3Client) {
        var bucket = "test";
        var repo = createRepo(s3Client, bucket);
        var objectId = "o1";

        repo.stageChanges(ObjectVersionId.head(objectId), commitInfo("Peter", "pwinckles@pm.me", "initial commit"), updater -> {
            updater.writeFile(stream("file1"), "dir/file1.txt");
            updater.writeFile(stream("file2"), "dir/sub/file2.txt");
            updater.writeFile(stream("file1"), "dir/sub/file3.txt");
        });

        assertObjectsExist(s3Client, bucket, "o1/", List.of(
                "o1/0=ocfl_object_1.0",
                "o1/inventory.json",
                "o1/inventory.json.sha512",
                "o1/v1/inventory.json",
                "o1/v1/inventory.json.sha512",
                "o1/extensions/mutable-head/root-inventory.json.sha512",
                "o1/extensions/mutable-head/revisions/r1",
                "o1/extensions/mutable-head/HEAD/inventory.json",
                "o1/extensions/mutable-head/HEAD/inventory.json.sha512",
                "o1/extensions/mutable-head/HEAD/content/r1/dir/file1.txt",
                "o1/extensions/mutable-head/HEAD/content/r1/dir/sub/file2.txt"
        ));

        assertEquals("file1", streamToString(repo.getObject(ObjectVersionId.head(objectId)).getFile("dir/file1.txt").getStream()));

        repo.commitStagedChanges(objectId, commitInfo("Peter", "winckles@wisc.edu", "commit"));

        assertObjectsExist(s3Client, bucket, "o1/", List.of(
                "o1/0=ocfl_object_1.0",
                "o1/inventory.json",
                "o1/inventory.json.sha512",
                "o1/v1/inventory.json",
                "o1/v1/inventory.json.sha512",
                "o1/v2/inventory.json",
                "o1/v2/inventory.json.sha512",
                "o1/v2/content/r1/dir/file1.txt",
                "o1/v2/content/r1/dir/sub/file2.txt"
        ));

        assertEquals("file2", streamToString(repo.getObject(ObjectVersionId.head(objectId)).getFile("dir/sub/file2.txt").getStream()));
    }

    private void assertObjectsExist(S3Client s3Client, String bucket, String prefix, Collection<String> expectedKeys) {
        var result = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .build());

        var actualKeys = result.contents().stream().map(S3Object::key).collect(Collectors.toList());

        assertThat(actualKeys, containsInAnyOrder(expectedKeys.toArray(String[]::new)));
    }

    private MutableOcflRepository createRepo(S3Client s3Client, String bucket) {
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());

        return new OcflRepositoryBuilder()
                .layoutConfig(DefaultLayoutConfig.flatUrlConfig())
                .prettyPrintJson()
                .contentPathConstraintProcessor(DefaultContentPathConstraints.cloud())
                .buildMutable(CloudOcflStorage.builder()
                        .cloudClient(new OcflS3Client(s3Client, bucket))
                        .workDir(tempDir)
                        .build(), tempDir);
    }

    private Path outputDir(String pathStr) {
        var path = Paths.get(pathStr);
        try {
            FileUtil.safeDeletePath(path);
            Files.createDirectories(path.getParent());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return path;
    }

    private InputStream stream(String value) {
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }

    private String streamToString(InputStream stream) {
        try {
            return new String(stream.readAllBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private CommitInfo commitInfo(String user, String address, String message) {
        return new CommitInfo().setUser(new User().setName(user).setAddress(address)).setMessage(message);
    }

}
