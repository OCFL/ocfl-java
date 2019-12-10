package edu.wisc.library.ocfl.aws;

import edu.wisc.library.ocfl.api.OcflFileRetriever;
import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.ObjectPaths;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.OcflVersion;
import edu.wisc.library.ocfl.core.concurrent.ExecutorTerminator;
import edu.wisc.library.ocfl.core.concurrent.ParallelProcess;
import edu.wisc.library.ocfl.core.extension.layout.config.LayoutConfig;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapper;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.RevisionId;
import edu.wisc.library.ocfl.core.path.constraint.BackslashPathSeparatorConstraint;
import edu.wisc.library.ocfl.core.path.constraint.NonEmptyFileNameConstraint;
import edu.wisc.library.ocfl.core.path.constraint.PathConstraintProcessor;
import edu.wisc.library.ocfl.core.path.constraint.RegexPathConstraint;
import edu.wisc.library.ocfl.core.storage.OcflStorage;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.NamasteTypeFile;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class S3OcflStorage implements OcflStorage {

    private static final Logger LOG = LoggerFactory.getLogger(S3OcflStorage.class);

    private boolean closed = false;
    private boolean initialized = false;

    private PathConstraintProcessor logicalPathConstraints;

    private S3Client s3Client;
    private String bucketName;
    private Path workDir;

    private S3OcflStorageInitializer initializer;
    private ObjectIdPathMapper objectIdPathMapper;
    private InventoryMapper inventoryMapper;
    private ParallelProcess parallelProcess; // TODO async s3client?
    private S3OcflFileRetriever.Builder fileRetrieverBuilder;

    private OcflVersion ocflVersion;

    /**
     * Create a new builder.
     *
     * @return builder
     */
    public static S3OcflStorageBuilder builder() {
        return new S3OcflStorageBuilder();
    }

    /**
     * Creates a new S3OcflStorage object.
     *
     * <p>{@link #initializeStorage} must be called before using this object.
     *
     * @see S3OcflStorageBuilder
     *
     * @param s3Client the client to use to interface with S3
     * @param bucketName the bucket the OCFL repository is located in
     * @param threadPoolSize The size of the object's thread pool, used when calculating digests
     * @param inventoryMapper mapper used to parse inventory files
     * @param initializer initializes a new OCFL repo
     */
    public S3OcflStorage(S3Client s3Client, String bucketName, int threadPoolSize, Path workDir,
                                 InventoryMapper inventoryMapper, S3OcflStorageInitializer initializer) {
        this.s3Client = Enforce.notNull(s3Client, "s3Client cannot be null");
        this.bucketName = Enforce.notBlank(bucketName, "bucketName cannot be blank");
        this.workDir = Enforce.notNull(workDir, "workDir cannot be null");
        this.inventoryMapper = Enforce.notNull(inventoryMapper, "inventoryMapper cannot be null");
        Enforce.expressionTrue(threadPoolSize > 0, threadPoolSize, "threadPoolSize must be greater than 0");
        this.parallelProcess = new ParallelProcess(ExecutorTerminator.addShutdownHook(Executors.newFixedThreadPool(threadPoolSize)));
        this.initializer = Enforce.notNull(initializer, "initializer cannot be null");

        // TODO move somewhere else
        this.logicalPathConstraints = PathConstraintProcessor.builder()
                .fileNameConstraint(new NonEmptyFileNameConstraint())
                .fileNameConstraint(RegexPathConstraint.mustNotContain(Pattern.compile("^\\.{1,2}$")))
                .charConstraint(new BackslashPathSeparatorConstraint())
                .build();

        this.fileRetrieverBuilder = S3OcflFileRetriever.builder().bucket(bucketName).s3Client(s3Client);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Inventory loadInventory(String objectId) {
        ensureOpen();

        var objectRootPath = objectRootPath(objectId);
        var tempDir = FileUtil.createTempDir(workDir, objectId);
        var localInventoryPath = tempDir.resolve(OcflConstants.INVENTORY_FILE);

        try {
            var inventory = downloadAndParseMutableInventory(objectRootPath, localInventoryPath);

            if (inventory != null) {
                ensureRootObjectHasNotChanged(inventory);
            } else {
                inventory = downloadAndParseInventory(objectRootPath, localInventoryPath);
            }

            return inventory;
        } finally {
            FileUtil.safeDeletePath(tempDir);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeNewVersion(Inventory inventory, Path stagingDir) {
        ensureOpen();

        if (inventory.hasMutableHead()) {
            storeNewMutableHeadVersion(inventory, stagingDir);
        } else {
            storeNewImmutableVersion(inventory, stagingDir);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, OcflFileRetriever> getObjectStreams(Inventory inventory, VersionId versionId) {
        ensureOpen();

        var version = inventory.ensureVersion(versionId);
        var algorithm = inventory.getDigestAlgorithm();

        var map = new HashMap<String, OcflFileRetriever>(version.getState().size());

        version.getState().forEach((digest, paths) -> {
            var srcPath = inventory.storagePath(digest);
            paths.forEach(path -> {
                map.put(path, fileRetrieverBuilder.build(srcPath, algorithm, digest));
            });
        });

        return map;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reconstructObjectVersion(Inventory inventory, VersionId versionId, Path stagingDir) {
        ensureOpen();

        var version = inventory.ensureVersion(versionId);
        var digestAlgorithm = inventory.getDigestAlgorithm().getJavaStandardName();

        parallelProcess.collection(version.getState().entrySet(), entry -> {
            var id = entry.getKey();
            var files = entry.getValue();
            var srcPath = inventory.storagePath(id);

            for (var logicalPath : files) {
                logicalPathConstraints.apply(logicalPath);
                var destination = Paths.get(FileUtil.pathJoinFailEmpty(stagingDir.toString(), logicalPath));

                FileUtil.createDirectories(destination.getParent());

                if (Thread.interrupted()) {
                    break;
                }

                try (var stream = new FixityCheckInputStream(streamFile(srcPath), digestAlgorithm, id)) {
                    Files.copy(stream, destination);
                    stream.checkFixity();
                } catch (IOException e) {
                    throw new RuntimeIOException(e);
                } catch (FixityCheckException e) {
                    throw new FixityCheckException(
                            String.format("File %s in object %s failed its fixity check.", logicalPath, inventory.getId()), e);
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeObject(String objectId) {
        ensureOpen();

        deletePath(objectRootPath(objectId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commitMutableHead(Inventory oldInventory, Inventory newInventory, Path stagingDir) {
        ensureOpen();

        ensureRootObjectHasNotChanged(newInventory);

        if (listObjectsUnderPrefix(ObjectPaths.mutableHeadVersionPath(newInventory.getObjectRootPath())).isEmpty()) {
            throw new ObjectOutOfSyncException(
                    String.format("Cannot commit mutable HEAD of object %s because a mutable HEAD does not exist.", newInventory.getId()));
        }

        var versionPath = objectVersionPath(newInventory, newInventory.getHead());
        ensureVersionDoesNotExist(newInventory, versionPath);

        var objectKeys = copyMutableVersionToImmutableVersion(oldInventory, newInventory);

        try {
            storeInventoryInS3WithRollback(newInventory, stagingDir, versionPath);

            try {
                purgeMutableHead(newInventory.getId());
            } catch (RuntimeException e) {
                LOG.error("Failed to cleanup mutable HEAD at {}. Must be deleted manually.",
                        ObjectPaths.mutableHeadExtensionRoot(newInventory.getObjectRootPath()), e);
            }
        } catch (RuntimeException e) {
            safeDeleteObjects(objectKeys);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeMutableHead(String objectId) {
        ensureOpen();

        deletePath(ObjectPaths.mutableHeadExtensionRoot(objectRootPath(objectId)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsObject(String objectId) {
        ensureOpen();

        return !listObjectsUnderPrefix(objectRootPath(objectId)).isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String objectRootPath(String objectId) {
        ensureOpen();

        return objectIdPathMapper.map(objectId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void initializeStorage(OcflVersion ocflVersion, LayoutConfig layoutConfig) {
        if (initialized) {
            return;
        }

        this.objectIdPathMapper = this.initializer.initializeStorage(bucketName, ocflVersion, layoutConfig);
        this.ocflVersion = ocflVersion;
        this.initialized = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        closed = true;
        parallelProcess.shutdown();
    }

    private void storeNewImmutableVersion(Inventory inventory, Path stagingDir) {
        ensureNoMutableHead(inventory);

        var versionPath = objectVersionPath(inventory, inventory.getHead());
        ensureVersionDoesNotExist(inventory, versionPath);

        String namasteFile = null;

        try {
            if (isFirstVersion(inventory)) {
                namasteFile = writeObjectNamasteFile(inventory.getObjectRootPath());
            }

            var objectKeys = storeContentInS3(inventory, stagingDir);
            // TODO write a copy to the cache?

            try {
                storeInventoryInS3WithRollback(inventory, stagingDir, versionPath);
            } catch (RuntimeException e) {
                safeDeleteObjects(objectKeys);
                throw e;
            }
        } catch (RuntimeException e) {
            // TODO this could be corrupt the object if another process is concurrently creating the same object
            if (namasteFile != null) {
                safeDeleteObjects(namasteFile);
            }
            throw e;
        }
    }

    private void storeNewMutableHeadVersion(Inventory inventory, Path stagingDir) {
        ensureRevisionDoesNotExist(inventory);

        var cleanupKeys = new ArrayList<String>(2);

        if (!listObjectsUnderPrefix(ObjectPaths.mutableHeadExtensionRoot(inventory.getObjectRootPath())).isEmpty()) {
            ensureRootObjectHasNotChanged(inventory);
        } else {
            cleanupKeys.add(copyRootInventorySidecarToMutableHead(inventory));
        }

        try {
            cleanupKeys.add(createRevisionMarker(inventory));

            var objectKeys = storeContentInS3(inventory, stagingDir);

            // TODO write a copy to the cache?

            try {
                // TODO if this fails the inventory may be left in a bad state
                storeMutableHeadInventoryInS3(inventory, stagingDir);
            } catch (RuntimeException e) {
                safeDeleteObjects(objectKeys);
                throw e;
            }
        } catch (RuntimeException e) {
            safeDeleteObjects(cleanupKeys);
            throw e;
        }

        deleteMutableHeadFilesNotInManifest(inventory);
    }

    private List<String> storeContentInS3(Inventory inventory, Path sourcePath) {
        var contentPrefix = contentPrefix(inventory);
        var fileIds = inventory.getFileIdsForMatchingFiles(contentPrefix);
        var objectKeys = Collections.synchronizedList(new ArrayList<String>());

        try {
            parallelProcess.collection(fileIds, fileId -> {
                var contentPath = inventory.ensureContentPath(fileId);
                var contentPathNoVersion = contentPath.substring(contentPath.indexOf(inventory.resolveContentDirectory()));
                var file = sourcePath.resolve(contentPathNoVersion);

                if (Files.notExists(file)) {
                    throw new IllegalStateException(String.format("Staged file %s does not exist", file));
                }

                var md5sum = inventory.getFixityForContentPath(contentPath)
                        .getOrDefault(DigestAlgorithm.md5, null);
                if (md5sum == null) {
                    md5sum = DigestUtil.computeDigest(DigestAlgorithm.md5, file);
                }

                // TODO I don't think there's any reason to perform an additional fixity check on the source file

                var key = inventory.storagePath(fileId);
                objectKeys.add(key);
                uploadFile(file, key, md5sum);
            });
        } catch (RuntimeException e) {
            // TODO I think there's a problem here with not waiting for cancelled tasks to complete
            safeDeleteObjects(objectKeys);
            throw e;
        }

        return objectKeys;
    }

    private List<String> copyMutableVersionToImmutableVersion(Inventory oldInventory, Inventory newInventory) {
        var contentPrefix = contentPrefix(newInventory);
        var fileIds = newInventory.getFileIdsForMatchingFiles(contentPrefix);
        var objectKeys = Collections.synchronizedList(new ArrayList<String>());

        try {
            parallelProcess.collection(fileIds, fileId -> {
                var srcPath = oldInventory.storagePath(fileId);
                var dstPath = newInventory.storagePath(fileId);
                objectKeys.add(dstPath);
                copyObject(srcPath, dstPath);
            });
        } catch (RuntimeException e) {
            // TODO I think there's a problem here with not waiting for cancelled tasks to complete
            safeDeleteObjects(objectKeys);
            throw e;
        }

        return objectKeys;
    }

    private void storeMutableHeadInventoryInS3(Inventory inventory, Path sourcePath) {
        uploadFile(ObjectPaths.inventoryPath(sourcePath),
                ObjectPaths.mutableHeadInventoryPath(inventory.getObjectRootPath()));
        uploadFile(ObjectPaths.inventorySidecarPath(sourcePath, inventory),
                ObjectPaths.mutableHeadInventorySidecarPath(inventory.getObjectRootPath(), inventory));
    }

    private void storeInventoryInS3WithRollback(Inventory inventory, Path sourcePath, String versionPath) {
        var srcInventoryPath = ObjectPaths.inventoryPath(sourcePath);
        var srcSidecarPath = ObjectPaths.inventorySidecarPath(sourcePath, inventory);
        var versionedInventoryPath = ObjectPaths.inventoryPath(versionPath);
        var versionedSidecarPath = ObjectPaths.inventorySidecarPath(versionPath, inventory);

        uploadFile(srcInventoryPath, versionedInventoryPath);
        uploadFile(srcSidecarPath, versionedSidecarPath);

        try {
            copyInventoryToRoot(versionPath, inventory);
        } catch (RuntimeException e) {
            rollbackInventory(inventory);
            safeDeleteObjects(versionedInventoryPath, versionedSidecarPath);
            throw e;
        }
    }

    private void rollbackInventory(Inventory inventory) {
        if (!isFirstVersion(inventory)) {
            try {
                var previousVersionPath = objectVersionPath(inventory, inventory.getHead().previousVersionId());
                copyInventoryToRoot(previousVersionPath, inventory);
            } catch (RuntimeException e) {
                LOG.error("Failed to rollback inventory at {}. Object must be fixed manually.",
                        ObjectPaths.inventoryPath(inventory.getObjectRootPath()), e);
            }
        }
    }

    private void copyInventoryToRoot(String versionPath, Inventory inventory) {
        copyObject(ObjectPaths.inventoryPath(versionPath),
                ObjectPaths.inventoryPath(inventory.getObjectRootPath()));
        copyObject(ObjectPaths.inventorySidecarPath(versionPath, inventory),
                ObjectPaths.inventorySidecarPath(inventory.getObjectRootPath(), inventory));
    }

    private String copyRootInventorySidecarToMutableHead(Inventory inventory) {
        var rootSidecarPath = ObjectPaths.inventorySidecarPath(inventory.getObjectRootPath(), inventory);
        var sidecarName = rootSidecarPath.substring(rootSidecarPath.lastIndexOf('/') + 1);
        return copyObject(rootSidecarPath,
                FileUtil.pathJoinFailEmpty(ObjectPaths.mutableHeadExtensionRoot(inventory.getObjectRootPath()), "root-" + sidecarName));
    }

    private Inventory downloadAndParseInventory(String objectRootPath, Path localPath) {
        try {
            var remotePath = ObjectPaths.inventoryPath(objectRootPath);
            downloadFile(remotePath, localPath);
            var inventory = inventoryMapper.read(objectRootPath, localPath);
            var expectedDigest = getDigestFromSidecar(ObjectPaths.inventorySidecarPath(objectRootPath, inventory));
            return verifyInventory(expectedDigest, localPath, inventory);
        } catch (NoSuchKeyException e) {
            // Doesn't exist; return null
            return null;
        }
    }

    private Inventory downloadAndParseMutableInventory(String objectRootPath, Path localPath) {
        try {
            var remotePath = ObjectPaths.mutableHeadInventoryPath(objectRootPath);
            downloadFile(remotePath, localPath);
            var revisionId = identifyLatestRevision(objectRootPath);
            var inventory = inventoryMapper.readMutableHead(objectRootPath, revisionId, localPath);
            var expectedDigest = getDigestFromSidecar(ObjectPaths.mutableHeadInventorySidecarPath(objectRootPath, inventory));
            return verifyInventory(expectedDigest, localPath, inventory);
        } catch (NoSuchKeyException e) {
            // Doesn't exist; return null
            return null;
        }
    }

    private String createRevisionMarker(Inventory inventory) {
        var revisionPath = FileUtil.pathJoinFailEmpty(ObjectPaths.mutableHeadExtensionRoot(inventory.getObjectRootPath()),
                "revisions", inventory.getRevisionId().toString());

        s3Client.putObject(PutObjectRequest.builder()
                .bucket(bucketName)
                .key(revisionPath)
                .build(), RequestBody.fromString(inventory.getRevisionId().toString(), StandardCharsets.UTF_8));

        return revisionPath;
    }

    private RevisionId identifyLatestRevision(String objectRootPath) {
        // TODO this is not implemented yet
        var revisionsPath = FileUtil.pathJoinFailEmpty(ObjectPaths.mutableHeadExtensionRoot(objectRootPath), "revisions");
        var revisions = listObjectsUnderPrefix(revisionsPath);

        RevisionId revisionId = null;

        for (var revisionStr : revisions) {
            var id = RevisionId.fromString(revisionStr);
            if (revisionId == null) {
                revisionId = id;
            } else if (revisionId.compareTo(id) < 1) {
                revisionId = id;
            }
        }

        return revisionId;
    }

    private void deleteMutableHeadFilesNotInManifest(Inventory inventory) {
        var keys = list(FileUtil.pathJoinFailEmpty(ObjectPaths.mutableHeadVersionPath(inventory.getObjectRootPath()),
                inventory.resolveContentDirectory()));
        var deleteKeys = new ArrayList<String>();

        keys.contents().forEach(o -> {
            var key = o.key();
            var contentPath = key.substring(inventory.getObjectRootPath().length() + 1);
            if (inventory.getFileId(contentPath) == null) {
                deleteKeys.add(key);
            }
        });

        safeDeleteObjects(deleteKeys);
    }

    private String contentPrefix(Inventory inventory) {
        if (inventory.hasMutableHead()) {
            return FileUtil.pathJoinFailEmpty(
                    OcflConstants.MUTABLE_HEAD_VERSION_PATH,
                    inventory.resolveContentDirectory(),
                    inventory.getRevisionId().toString());
        }

        return inventory.getHead().toString();
    }

    private void ensureNoMutableHead(Inventory inventory) {
        // TODO this could be incorrect due to eventual consistency issues
        if (!listObjectsUnderPrefix(ObjectPaths.mutableHeadVersionPath(inventory.getObjectRootPath())).isEmpty()) {
            // TODO modeled exception?
            throw new IllegalStateException(String.format("Cannot create a new version of object %s because it has an active mutable HEAD.",
                    inventory.getId()));
        }
    }

    private void ensureVersionDoesNotExist(Inventory inventory, String versionPath) {
        if (!listObjectsUnderPrefix(versionPath).isEmpty()) {
            throw new ObjectOutOfSyncException(
                    String.format("Failed to create a new version of object %s. Changes are out of sync with the current object state.", inventory.getId()));
        }
    }

    private void ensureRevisionDoesNotExist(Inventory inventory) {
        var latestRevision = identifyLatestRevision(inventory.getObjectRootPath());
        if (latestRevision != null && latestRevision.compareTo(inventory.getRevisionId()) >= 0) {
            throw new ObjectOutOfSyncException(
                    String.format("Failed to update mutable HEAD of object %s. Changes are out of sync with the current object state.", inventory.getId()));
        }
    }

    private void ensureRootObjectHasNotChanged(Inventory inventory) {
        var savedDigest = getDigestFromSidecar(FileUtil.pathJoinFailEmpty(ObjectPaths.mutableHeadExtensionRoot(inventory.getObjectRootPath()),
                "root-" + OcflConstants.INVENTORY_FILE + "." + inventory.getDigestAlgorithm().getOcflName()));
        var rootDigest = getDigestFromSidecar(ObjectPaths.inventorySidecarPath(inventory.getObjectRootPath(), inventory));

        if (!savedDigest.equalsIgnoreCase(rootDigest)) {
            throw new ObjectOutOfSyncException(
                    String.format("The mutable HEAD of object %s is out of sync with the root object state.", inventory.getId()));
        }
    }

    private String getDigestFromSidecar(String sidecarPath) {
        try {
            var sidecarContents = s3Client.getObjectAsBytes(GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(sidecarPath)
                    .build()).asUtf8String();
            var parts = sidecarContents.split("\\s");
            if (parts.length == 0) {
                throw new CorruptObjectException("Invalid inventory sidecar file: " + sidecarPath);
            }
            return parts[0];
        } catch (NoSuchKeyException e) {
            throw new CorruptObjectException("Missing inventory sidecar: " + sidecarPath, e);
        }
    }

    private Inventory verifyInventory(String expectedDigest, Path inventoryPath, Inventory inventory) {
        var algorithm = inventory.getDigestAlgorithm();
        var actualDigest = DigestUtil.computeDigest(algorithm, inventoryPath);

        if (!expectedDigest.equalsIgnoreCase(actualDigest)) {
            throw new FixityCheckException(String.format("Invalid inventory file: %s. Expected %s digest: %s; Actual: %s",
                    inventoryPath, algorithm.getOcflName(), expectedDigest, actualDigest));
        }

        return inventory;
    }

    private String objectVersionPath(Inventory inventory, VersionId versionId) {
        return FileUtil.pathJoinFailEmpty(inventory.getObjectRootPath(), versionId.toString());
    }

    private boolean isFirstVersion(Inventory inventory) {
        return inventory.getVersions().size() == 1;
    }

    private String writeObjectNamasteFile(String objectRootPath) {
        var namasteFile = new NamasteTypeFile(ocflVersion.getOcflObjectVersion());
        var key = FileUtil.pathJoinFailEmpty(objectRootPath, namasteFile.fileName());
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build(), RequestBody.fromString(namasteFile.fileContent()));
        return key;
    }

    private String uploadFile(Path localPath, String remotePath) {
        var md5sum = DigestUtil.computeDigest(DigestAlgorithm.md5, localPath);
        return uploadFile(localPath, remotePath, md5sum);
    }

    private String uploadFile(Path localPath, String remotePath, String md5digest) {
        LOG.debug("Uploading {} to bucket {} key {}", localPath, bucketName, remotePath);
        // TODO multipart upload for large files
        try {
            s3Client.putObject(PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(remotePath)
                    // TODO inefficient...
                    .contentMD5(Base64.encodeBase64String(Hex.decodeHex(md5digest)))
                    .build(), localPath);
            return remotePath;
        } catch (DecoderException e) {
            throw new RuntimeException(e);
        }
    }

    private String copyObject(String srcPath, String dstPath) {
        LOG.debug("Copying {} to {} in bucket {}", srcPath, dstPath, bucketName);
        // TODO this doesn't work for objects >= 5 gb
        s3Client.copyObject(CopyObjectRequest.builder()
                .bucket(bucketName)
                .copySource(keyWithBucketName(srcPath))
                .key(dstPath)
                .build());

        return dstPath;
    }

    private void downloadFile(String remotePath, Path localPath) {
        LOG.debug("Downloading bucket {} key {} to {}", bucketName, remotePath, localPath);
        s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(remotePath)
                .build(), localPath);
    }

    private InputStream streamFile(String remotePath) {
        LOG.debug("Streaming from bucket {} key {}", bucketName, remotePath);
        return s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(remotePath)
                .build());
    }

    private List<String> listObjectsUnderPrefix(String path) {
        return listDirectory(path).contents().stream()
                .map(S3Object::key)
                .map(k -> path.isEmpty() ? k : k.substring(path.length()))
                .collect(Collectors.toList());
    }

    private ListObjectsV2Response listDirectory(String path) {
        var prefix = path;

        if (!prefix.isEmpty() && !prefix.endsWith("/")) {
            prefix = prefix + "/";
        }

        LOG.debug("Listing directory {} in bucket {}", prefix, bucketName);

        return s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucketName)
                .delimiter("/")
                .prefix(prefix)
                .build());
    }

    private ListObjectsV2Response list(String path) {
        return s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(path)
                .build());
    }

    private void deletePath(String path) {
        LOG.debug("Deleting path {} in bucket {}", path, bucketName);

        var keys = list(path).contents().stream()
                .map(S3Object::key)
                .collect(Collectors.toList());

        deleteObjects(keys);
    }

    private void deleteObjects(Collection<String> objectKeys) {
        LOG.debug("Deleting objects in bucket {}: {}", bucketName, objectKeys);

        if (!objectKeys.isEmpty()) {
            var objectIds = objectKeys.stream()
                    .map(key -> ObjectIdentifier.builder().key(key).build())
                    .collect(Collectors.toList());

            s3Client.deleteObjects(DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(Delete.builder()
                            .objects(objectIds)
                            .build())
                    .build());
        }
    }

    private void safeDeleteObjects(String... objectKeys) {
        safeDeleteObjects(Arrays.asList(objectKeys));
    }

    private void safeDeleteObjects(Collection<String> objectKeys) {
        try {
            deleteObjects(objectKeys);
        } catch (RuntimeException e) {
            LOG.error("Failed to cleanup objects in bucket {}: {}", bucketName, objectKeys, e);
        }
    }

    private String keyWithBucketName(String key) {
        // TODO not sure if the bucket name needs to be rooted
        return SdkHttpUtils.urlEncode(String.format("%s/%s", bucketName, key));
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException(this.getClass().getName() + " is closed.");
        }

        if (!initialized) {
            throw new IllegalStateException(this.getClass().getName() + " must be initialized before it can be used.");
        }
    }

}
