/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 University of Wisconsin Board of Regents
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package edu.wisc.library.ocfl.core.storage.cloud;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.extension.ExtensionSupportEvaluator;
import edu.wisc.library.ocfl.core.extension.UnsupportedExtensionBehavior;
import edu.wisc.library.ocfl.core.util.ObjectMappers;

import java.util.Collections;
import java.util.Set;

/**
 * Builder for constructing S3OcflStorage objects. It is configured with sensible defaults and can minimally be
 * used as {@code new S3OcflStorageBuilder().s3Client(s3Client).workDir(workDir).build(bucketName).}
 */
public class CloudOcflStorageBuilder {

    private ObjectMapper objectMapper;
    private CloudClient cloudClient;
    private CloudOcflStorageInitializer initializer;
    private UnsupportedExtensionBehavior unsupportedBehavior;
    private Set<String> ignoreUnsupportedExtensions;

    public CloudOcflStorageBuilder() {
        objectMapper = ObjectMappers.prettyPrintMapper();
        unsupportedBehavior = UnsupportedExtensionBehavior.FAIL;
        ignoreUnsupportedExtensions = Collections.emptySet();
    }

    /**
     * Sets the cloud client. This must be set prior to calling build().
     *
     * @param cloudClient the client to use to interface with cloud storage such as S3
     * @return builder
     */
    public CloudOcflStorageBuilder cloudClient(CloudClient cloudClient) {
        this.cloudClient = cloudClient;
        return this;
    }

    /**
     * Set the behavior when an unsupported extension is encountered. By default, ocfl-java will not operate on
     * repositories or objects that contain unsupported extensions. Set this value to WARN, if you'd like ocfl-java
     * to log a WARNing, but continue to operate instead.
     * <p>
     * Specific unsupported extensions may be ignored individually using {@code ignoreUnsupportedExtensions}
     *
     * @param unsupportedBehavior FAIL to throw an exception or WARN to log a warning
     * @return builder
     */
    public CloudOcflStorageBuilder unsupportedExtensionBehavior(UnsupportedExtensionBehavior unsupportedBehavior) {
        this.unsupportedBehavior = Enforce.notNull(unsupportedBehavior, "unsupportedExtensionBehavior cannot be null");
        return this;
    }

    /**
     * Sets a list of unsupported extensions that should be ignored. If the unsupported extension behavior
     * is set to FAIL, this means that these extensions will produce log WARNings if they are encountered. If
     * the behavior is set to WARN, then these extensions will be silently ignored.
     *
     * @param ignoreUnsupportedExtensions set of unsupported extension names that should be ignored
     * @return builder
     */
    public CloudOcflStorageBuilder ignoreUnsupportedExtensions(Set<String> ignoreUnsupportedExtensions) {
        this.ignoreUnsupportedExtensions = Enforce.notNull(ignoreUnsupportedExtensions, "ignoreUnsupportedExtensions cannot be null");
        return this;
    }

    /**
     * Overrides the default ObjectMapper that's used to serialize ocfl_layout.json
     *
     * @param objectMapper object mapper
     * @return builder
     */
    public CloudOcflStorageBuilder objectMapper(ObjectMapper objectMapper) {
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
        return this;
    }

    /**
     * Overrides the default {@link CloudOcflStorageInitializer}. Normally, this does not need to be set.
     *
     * @param initializer the initializer
     * @return builder
     */
    public CloudOcflStorageBuilder initializer(CloudOcflStorageInitializer initializer) {
        this.initializer = initializer;
        return this;
    }

    /**
     * @return a new {@link CloudOcflStorage} object
     */
    public CloudOcflStorage build() {
        var supportEvaluator = new ExtensionSupportEvaluator(unsupportedBehavior, ignoreUnsupportedExtensions);

        var init = initializer;
        if (init == null) {
            init = new CloudOcflStorageInitializer(cloudClient, objectMapper, supportEvaluator);
        }

        return new CloudOcflStorage(cloudClient, init, supportEvaluator);
    }

}
