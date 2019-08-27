package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.OcflObjectReader;
import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.model.VersionDetails;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.Version;
import edu.wisc.library.ocfl.core.model.VersionId;
import edu.wisc.library.ocfl.core.storage.OcflStorage;
import edu.wisc.library.ocfl.core.util.ResponseMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class DefaultOcflObjectReader implements OcflObjectReader, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultOcflObjectReader.class);

    private OcflStorage storage;

    private Inventory inventory;
    private Version version;
    private VersionId versionId;
    private Path stagingDir;

    private ResponseMapper responseMapper;
    private Set<InputStream> streams;
    private VersionDetails versionDetails;

    public DefaultOcflObjectReader(OcflStorage storage, Inventory inventory, VersionId versionId, Path stagingDir) {
        this.storage = Enforce.notNull(storage, "storage cannot be null");
        this.inventory = Enforce.notNull(inventory, "inventory cannot be null");
        this.versionId = Enforce.notNull(versionId, "versionId cannot be null");
        this.stagingDir = Enforce.notNull(stagingDir, "stagingDir cannot be null");

        version = inventory.getVersion(versionId);
        responseMapper = new ResponseMapper();
        streams = new HashSet<>();
    }

    @Override
    public VersionDetails describeVersion() {
        if (versionDetails == null) {
            versionDetails = responseMapper.mapVersion(inventory, versionId.toString(), version);
        }
        return versionDetails;
    }

    @Override
    public Collection<String> listFiles() {
        return Collections.unmodifiableCollection(version.listPaths());
    }

    @Override
    public OcflObjectReader getFile(String sourcePath, Path destinationPath, OcflOption... ocflOptions) {
        Enforce.notBlank(sourcePath, "sourcePath cannot be blank");
        Enforce.notNull(destinationPath, "destinationPath cannot be null");

        var fileId = lookupFileId(sourcePath);

        storage.retrieveFile(inventory, fileId, destinationPath, ocflOptions);

        return this;
    }

    @Override
    public InputStream getFile(String sourcePath) {
        Enforce.notBlank(sourcePath, "sourcePath cannot be blank");

        var fileId = lookupFileId(sourcePath);

        var stagingPath = stagingDir.resolve(Paths.get(inventory.getContentDirectory(), sourcePath));

        if (!Files.exists(stagingPath)) {
            storage.retrieveFile(inventory, fileId, stagingPath);
        }

        try {
            var stream = Files.newInputStream(stagingPath);
            streams.add(stream);
            return stream;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String lookupFileId(String sourcePath) {
        var fileId = version.getFileId(sourcePath);

        if (fileId == null) {
            throw new IllegalArgumentException(String.format("File %s does not exist in object %s.", sourcePath, inventory.getId()));
        }

        return fileId;
    }

    @Override
    public void close() throws Exception {
        for (var stream : streams) {
            try {
                stream.close();
            } catch (IOException e) {
                // swallow so that the rest of the streams are closed
                LOG.warn("Failed to close stream.", e);
            }
        }
    }

}
