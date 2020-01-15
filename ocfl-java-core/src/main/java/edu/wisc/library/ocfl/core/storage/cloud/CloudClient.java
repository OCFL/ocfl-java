package edu.wisc.library.ocfl.core.storage.cloud;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collection;

/**
 * Wrapper interface abstracting cloud provider clients
 */
public interface CloudClient {

    /**
     * The name of the bucket the OCFL repository is in.
     * 
     * @return bucket name
     */
    String bucket();

    /**
     * The key prefix of all objects within the OCFL repository. This may be empty. Multiple different OCFL repositories
     * may be stored in the same bucket if they use different key prefixes.
     *
     * @return the key prefix common to all objects in the repository
     */
    String prefix();
    
    /**
     * Uploads a file to the destination, and returns the object key. A md5 digest of the file is calculated prior to
     * initiating the upload, and is used for transmission fixity.
     *
     * @param srcPath src file
     * @param dstPath object path
     * @return object key
     */
    CloudObjectKey uploadFile(Path srcPath, String dstPath);

    /**
     * Uploads a file to the destination, and returns the object key. If the md5 digest is null, it is calculated prior to
     * upload.
     *
     * @param srcPath src file
     * @param dstPath object path
     * @param md5digest the md5 digest of the file to upload or null
     * @return object key
     */
    CloudObjectKey uploadFile(Path srcPath, String dstPath, byte[] md5digest);

    /**
     * Uploads an object with byte content
     *
     * @param dstPath object path
     * @param bytes the object content
     * @return object key
     */
    CloudObjectKey uploadBytes(String dstPath, byte[] bytes, String contentType);

    /**
     * Copies an object from one location to another within the same bucket.
     *
     * @param srcPath source object key
     * @param dstPath destination object path
     * @return the destination key
     * @throws KeyNotFoundException when srcPath not found
     */
    CloudObjectKey copyObject(String srcPath, String dstPath);

    /**
     * Downloads an object to the local filesystem.
     *
     * @param srcPath object key
     * @param dstPath path to write the file to
     * @return the destination path
     * @throws KeyNotFoundException when srcPath not found
     */
    Path downloadFile(String srcPath, Path dstPath);

    /**
     * Downloads and object and performs a fixity check as it streams to disk.
     *
     * @param srcPath object key
     * @return stream of object content
     * @throws KeyNotFoundException when srcPath not found
     */
    InputStream downloadStream(String srcPath);

    /**
     * Downloads an object to a string.
     *
     * @param srcPath object key
     * @return string content of the object
     * @throws KeyNotFoundException when srcPath not found
     */
    String downloadString(String srcPath);

    /**
     * Lists all of the keys under a prefix. No delimiter is used.
     *
     * @param prefix the key prefix
     * @return list response
     */
    ListResult list(String prefix);

    /**
     * Lists all of the keys within a virtual directory. Only keys that fall between the specified prefix and the next
     * '/' are returned.
     *
     * @param path the key prefix to list, if it does not end in a '/' one is appended
     * @return list response
     */
    ListResult listDirectory(String path);

    /**
     * Deletes all of the objects under the specified path
     *
     * @param path the path prefix to delete
     */
    void deletePath(String path);

    /**
     * Deletes all of the specified objects. If an object does not exist, nothing happens.
     *
     * @param objectKeys keys to delete
     */
    void deleteObjects(Collection<String> objectKeys);

    /**
     * Deletes all of the objects and does not throw an exception on failure
     *
     * @param objectKeys keys to delete
     */
    void safeDeleteObjects(String... objectKeys);

    /**
     * Deletes all of the objects and does not throw an exception on failure
     *
     * @param objectKeys keys to delete
     */
    void safeDeleteObjects(Collection<String> objectKeys);

    /**
     * Returns true if the storage bucket exists
     *
     * @return if the bucket exists
     */
    boolean bucketExists();

}
