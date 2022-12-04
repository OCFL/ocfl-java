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

package edu.wisc.library.ocfl.api;

import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.OverwriteException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.VersionNum;
import java.io.InputStream;
import java.nio.file.Path;

/**
 * Exposes methods for selectively updating a specific OCFL object.
 */
public interface OcflObjectUpdater {

    /**
     * Adds a file or directory to the object. If the path is a file, it is inserted in the object using its filename. If
     * it's a directory, the contents of the directory are inserted into the object's root.
     *
     * <p>By default, files are copied into the OCFL repository. If {@link OcflOption#MOVE_SOURCE} is specified, then
     * files will be moved instead. Warning: If an exception occurs and the new version is not created, the files that were
     * will be lost. This operation is more efficient but less safe than the default copy.
     *
     * <p>By default, the change will be rejected if there is an existing file in an object at a logical path.
     * To overwrite, specify {@link OcflOption#OVERWRITE}.
     *
     * @param sourcePath the local file or directory to add to the object
     * @param options optional config options. Use {@link OcflOption#MOVE_SOURCE} to move files into the repo instead of copying.
     *                    Use {@link OcflOption#OVERWRITE} to overwrite existing files within an object
     * @return this
     * @throws OverwriteException if there is already a file at the destinationPath and {@link OcflOption#OVERWRITE} was
     *                            not specified
     */
    OcflObjectUpdater addPath(Path sourcePath, OcflOption... options);

    /**
     * Adds a file or directory to the object at the specified destinationPath. The destinationPath is the logical path
     * to the file within the object. Forward slashes MUST be used as filename separators in the path. It is important to
     * keep this in mind on Windows systems, where backslashes MUST be converted to forward slashes.
     *
     * <p>By default, files are copied into the OCFL repository. If {@link OcflOption#MOVE_SOURCE} is specified, then
     * files will be moved instead. Warning: If an exception occurs and the new version is not created, the files that were
     * will be lost. This operation is more efficient but less safe than the default copy.
     *
     * <p>By default, the change will be rejected if there is already a file in the object at the destinationPath.
     * To overwrite, specify {@link OcflOption#OVERWRITE}.
     *
     * @param sourcePath the local file or directory to add to the object
     * @param destinationPath the logical path to store the sourcePath at within the object, an empty string indicates the object root
     * @param options optional config options. Use {@link OcflOption#MOVE_SOURCE} to move files into the repo instead of copying.
     *                    Use {@link OcflOption#OVERWRITE} to overwrite existing files within an object
     * @return this
     * @throws OverwriteException if there is already a file at the destinationPath and {@link OcflOption#OVERWRITE} was
     *                            not specified
     */
    OcflObjectUpdater addPath(Path sourcePath, String destinationPath, OcflOption... options);

    /**
     * Adds a file to the object at the specified destinationPath. The destinationPath is the logical path
     * to the file within the object. Forward slashes MUST be used as filename separators in the path. It is important to
     * keep this in mind on Windows systems, where backslashes MUST be converted to forward slashes.
     *
     * <p>This method differs from addPath() in that it DOES NOT calculate the file's digest. The digest must be
     * provided and it MUST use the same algorithm as the object's content digest algorithm. If a different algorithm
     * is used or the digest is wrong, then the OCFL object will be corrupted. This method should only be used when
     * performance is critical and the necessary digest was already calculated elsewhere.
     *
     * <p>By default, files are copied into the OCFL repository. If {@link OcflOption#MOVE_SOURCE} is specified, then
     * files will be moved instead. Warning: If an exception occurs and the new version is not created, the files that were
     * will be lost. This operation is more efficient but less safe than the default copy.
     *
     * <p>By default, the change will be rejected if there is already a file in the object at the destinationPath.
     * To overwrite, specify {@link OcflOption#OVERWRITE}.
     *
     * @param digest the digest of the file. The digest MUST use the same algorithm as the object's content digest algorithm
     * @param sourcePath the local file to add to the object
     * @param destinationPath the logical path to store the sourcePath at within the object, an empty string indicates the object root
     * @param options optional config options. Use {@link OcflOption#MOVE_SOURCE} to move files into the repo instead of copying.
     *                    Use {@link OcflOption#OVERWRITE} to overwrite existing files within an object
     * @return this
     * @throws OverwriteException if there is already a file at the destinationPath and {@link OcflOption#OVERWRITE} was
     *                            not specified
     */
    OcflObjectUpdater unsafeAddPath(String digest, Path sourcePath, String destinationPath, OcflOption... options);

    /**
     * Writes the contents of the InputStream to the object at the specified destinationPath. The destinationPath is the
     * logical path to the file within the object. Forward slashes MUST be used as filename separators in the path. It is
     * important to keep this in mind on Windows systems, where backslashes MUST be converted to forward slashes.
     *
     * <p>Pass a {@link edu.wisc.library.ocfl.api.io.FixityCheckInputStream} to ensure transmission fixity.
     *
     * <p>By default, the change will be rejected if there is already a file in the object at the destinationPath.
     * To overwrite, specify {@link OcflOption#OVERWRITE}.
     *
     * @param input InputStream containing the content of a file to add to an object
     * @param destinationPath the logical path to store the file at within the object
     * @param options optional config options. Use {@link OcflOption#OVERWRITE} to overwrite existing files within
     *                    an object
     * @return this
     * @throws OverwriteException if there is already a file at the destinationPath and {@link OcflOption#OVERWRITE} was
     *                            not specified
     * @throws FixityCheckException if the a FixityCheckInputStream is used and the digest does not match the expected value
     */
    OcflObjectUpdater writeFile(InputStream input, String destinationPath, OcflOption... options);

    /**
     * Removes a file from the object. An exception is not thrown if there is nothing at the path.
     *
     * @param path the logical path of the file to remove
     * @return this
     */
    OcflObjectUpdater removeFile(String path);

    /**
     * Renames an existing file within the object. Use {@link OcflOption#OVERWRITE} to overwrite an existing file at the
     * destinationPath. The destinationPath is  the logical path to the file within the object. Forward slashes MUST be
     * used as filename separators in the path. It is important to keep this in mind on Windows systems, where backslashes
     * MUST be converted to forward slashes.
     *
     * @param sourcePath the logical path to the file to be renamed
     * @param destinationPath the local path to rename the file to
     * @param options optional config options. Use {@link OcflOption#OVERWRITE} to overwrite existing files within
     *                    an object
     * @return this
     * @throws OverwriteException if there is already a file at the destinationPath and {@link OcflOption#OVERWRITE} was
     *                            not specified
     */
    OcflObjectUpdater renameFile(String sourcePath, String destinationPath, OcflOption... options);

    /**
     * Reinstates a file that existed in any version of the object into the current version. This is useful when recovering
     * a prior version of a file or adding back a file that was deleted. Use {@link OcflOption#OVERWRITE} to overwrite
     * an existing file at the destinationPath. The destinationPath is the logical path to the file within the object.
     * Forward slashes MUST be used as filename separators in the path. It is important to keep this in mind on Windows
     * systems, where backslashes MUST be converted to forward slashes.
     *
     * @param sourceVersionNum the version number of the version to reinstate the sourcePath from. Cannot be the current version
     * @param sourcePath the logical path to the file to be reinstated
     * @param destinationPath the logical path to reinstate the file to
     * @param options optional config options. Use {@link OcflOption#OVERWRITE} to overwrite existing files within
     *                    an object
     * @return this
     * @throws OverwriteException if there is already a file at the destinationPath and {@link OcflOption#OVERWRITE} was
     *                            not specified
     */
    OcflObjectUpdater reinstateFile(
            VersionNum sourceVersionNum, String sourcePath, String destinationPath, OcflOption... options);

    /**
     * The state of the current version of the object is cleared so that it does not reference any files. No files are deleted.
     * This can be useful to simulate {@link OcflRepository#putObject} like behavior.
     *
     * @return this
     */
    OcflObjectUpdater clearVersionState();

    /**
     * Adds an entry to the object's fixity block. The fixity block is purely for informational and migration purposes.
     * It is entirely optional, and is not OCFL's primary fixity mechanism.
     *
     * <p>NOTE: This method should only be called for files that are added in the same {@link OcflRepository#updateObject} call.
     * If it's called for a file that was not newly added, it will check to see if the file already has an associated fixity
     * digest with the same algorithm and compare the digest values. If the file does not have a pre-existing value, an
     * exception is thrown.
     *
     * <p>The digest of the file is calculated using the specified algorithm, and, if it doesn't match the expected value,
     * a {@link FixityCheckException} is thrown.
     *
     * @param logicalPath the logical path of the file to add fixity information for
     * @param algorithm the digest algorithm
     * @param value the expected digest value
     * @return this
     * @throws FixityCheckException if the computed digest of the file does not match the expected value
     */
    OcflObjectUpdater addFileFixity(String logicalPath, DigestAlgorithm algorithm, String value);

    /**
     * Clears the object's fixity block. The fixity block is primarily used for legacy migrations, and is not the primary
     * OCFL fixity mechanism. The fixity block can be cleared when it is no longer needed.
     *
     * @return this
     */
    OcflObjectUpdater clearFixityBlock();

    // TODO add api for purging a file in an object

}
