# OCFL Java

This project is a work-in-progress Java implementation of the [OCFL spec](https://ocfl.io).

**Disclaimer**: This library is under active development, and the OCFL specification has not been finalized. I will be making
breaking changes up until a 1.0.0 release. I do not recommend using this library in a production setting until then.

Version 1.0.0 of the OCFL spec has been finalized. However, I am waiting until [storage layout extensions](https://github.com/OCFL/extensions)
are formally defined before releasing version 1.0.0 of this library.  

[![Build Status](https://travis-ci.com/UW-Madison-Library/ocfl-java.svg?branch=master)](https://travis-ci.com/UW-Madison-Library/ocfl-java)

## Requirements and Installation

`ocfl-java` is a Java 11 project and at the minimum requires Java 11 to run.

The `ocfl-java` libraries are not yet built to Maven central. Until they are, you'll need to build the
libraries locally as follows:

```bash
$ git clone git@github.com:UW-Madison-Library/ocfl-java.git
$ cd ocfl-java
$ mvn install
```

After building the libraries locally, add the following to your project's POM:

```xml
<dependency>
    <groupId>edu.wisc.library.ocfl</groupId>
    <artifactId>ocfl-java-core</artifactId>
    <version>0.0.6-SNAPSHOT</version>
</dependency>
```

Add the following if you'd like to use Amazon S3 for the storage layer:

```xml
<dependency>
    <groupId>edu.wisc.library.ocfl</groupId>
    <artifactId>ocfl-java-aws</artifactId>
    <version>0.0.6-SNAPSHOT</version>
</dependency>
```

## Example Usage

```java
var repoDir = Paths.get("ocfl-repo"); // This directory contains the OCFL storage root.
var workDir = Paths.get("ocfl-work"); // This directory is used to assemble OCFL versions. It cannot be within the OCFL storage root.

var repo = new OcflRepositoryBuilder()
        .layoutConfig(new HashedTruncatedNTupleConfig())
        .storage(FileSystemOcflStorage.builder().repositoryRoot(repoDir).build())
        .workDir(workDir)
        .build();

repo.putObject(ObjectVersionId.head("o1"), Paths.get("object-out-dir"), new VersionInfo().setMessage("initial commit"));
repo.getObject(ObjectVersionId.head("o1"), Paths.get("object-in-dir"));

repo.updateObject(ObjectVersionId.head("o1"), new VersionInfo().setMessage("update"), updater -> {
    updater.addPath(Paths.get("path-to-file2"), "file2")
            .removeFile("file1")
            .addPath(Paths.get("path-to-file3"), "dir1/file3");
});

// Contains object details and lazy-load resource handles
var objectVersion = repo.getObject(ObjectVersionId.version("o1", "v1"));
```

## OCFL Repository Configuration

Use the `OcflRepositoryBuilder` to construct `OcflRepository` instances. You should only create a single `OcflRepository`
instance per OCFL repository, and you should not reuse the same `OcflRepositoryBuilder` to create multile `OcflRepository`
instances.

The `OcflRepositoryBuilder` will initialize a new OCFL repository if it's pointed at an empty directory, or open an existing
repository if it's pointed at an existing OCFL storage root.

Use `OcflRepositoryBuilder.build()` to construct standard OCFL repository and `OcflRepositoryBuilder.buildMutable()` to
construct an OCFL repository that supports the mutable HEAD extension.

### Required Properties

* **storage**: Sets the storage layer implementation that the OCFL repository should use. Use `FileSystemOcflStorage.builder()`
or `CloudOcflStorage.builder()` to create the `OcflStorage` implementation.
* **workDir**: Sets the path to the directory that is used to assemble OCFL versions. If you are using filesystem storage,
it is critical that this directory is located on the same volume as the OCFL storage root.
* **layoutConfig**: Configures the storage layout the OCFL repository uses. This is the method it uses to map object ids
to directories under the OCFL storage root. The layout configuration must be set when creating new OCFL repositories, but is
not required when opening an existing repository. Storage layouts must be defined in by an OCFL extension. Currently,
the following extensions are implemented:
  * **0003-hashed-n-tuple-trees**: `HashedTruncatedNTupleConfig`
  * **0005-hashed-n-tuple-id-layout**: `HashedTruncatedNTupleIdConfig`
  * **0006-flat-layout**: `FlatLayoutConfig`
 
### Optional Properties

* **ocflConfig**: Sets the following default values that are used for creating new OCFL objects: version, digest algorithm,
and content directory. The defaults are `v1`, `sha512`, and `content`.
* **prettyPrintJson**: Enables pretty print JSON in newly written inventory files. By default, pretty printing is disabled
 to reduce inventory file size.
* **contentPathConstraints**: Configures what file name constraints are enforced on OCFL content paths. By default,
there are no special constraints applied. Used `ContentPathConstraints` for a selection of preconfigured defaults.
You may want to apply constraints if you are concerned about portability between filesystems. For example, disallowing `:`
and `\` characters.
* **logicalPathMapper**: `LogicalPathMapper` implementations are used to map logical paths to safe content paths. By default,
 logical paths are mapped directly to content paths without making any changes. See `LogicalPathMappers` for more pre-configured
 options, such as `LogicalPathMappers.percentEncodingWindowsMapper()`, which percent-encodes a handful of characters that
 are problematic on Windows.
* **digestThreadPoolSize**: Sets the size of the thread pool that's used to calculate digests. By default, the thread pool
size is equal to the number of available processors.
* **copyThreadPoolSize**: Sets the size of the thread pool that's used for copying/moving files. By default, the thread pool
size is equal to twice the number of available processors.
* **inventoryCache**: By default, an in-memory [Caffeine](https://github.com/ben-manes/caffeine) cache is used to cache
deserialized inventories.
* **objectLock**: Set the lock implementation that's used to lock objects for writing. By default, it is an in-memory lock
with a 10 second wait to acquire. Use `ObjectLockBuilder` construct an alternate lock. When more than one processes may be
concurrently writing to an OCFL repository or when using cloud storage, a different implementation, such as `PostgresObjectLock`,
should be used.
* **objectDetailsDb**: Configures a database to use to store OCFL object metadata. By default, this feature is not used.
It should be used when using cloud storage. Use `ObjectDetailsDatabaseBuilder` to construct an `ObjectDetailsDatabase`.

## Storage Implementations

### Filesystem

The basic OCFL repository implementation stores objects under an OCFL storage root on a locally attached filesystem. The
client constructs new object versions in a work directory before attempting to move them into the object root. Ideally,
this move operation can be executed as an atomic rename, and, as such, the work directory configured on the `ocfl-java`
client should be located on the same mount as the OCFL storage root.

### Configuration

Use `FileSystemOcflStorage.builder()` to create and configure an `OcflStorage` instnace.

* **repositoryRoot**: Required, path to the OCFL storage root directory.
* **threadPoolSize**: Optional, sets the size of the storage impls thread pool. By default, the thread pool size is equal
to the number of available processors.
* **checkNewVersionFixity**: Optional, instructs the client to check the fixity of every file in new versions once the version
is at rest. By default, this is disabled and it is unlikely to be worthwhile if the work directory and OCFL storage root are
on the same mount, as recommended.

**Example**

```java
var repo = new OcflRepositoryBuilder()
        .layoutConfig(new HashedTruncatedNTupleConfig())
        .storage(FileSystemOcflStorage.builder()
                .repositoryRoot(repoDir)
                .build())
        .workDir(workDir)
        .build();
```

### Amazon S3

The Amazon S3 storage implementation stores OCFL objects directly in an Amazon S3 bucket. Optionally, a key prefix can be
used to partition the repository to use only a portion of its bucket, allowing you to store multiple OCFL repositories
in the same bucket or non-OCFL content.

At the minimum, the client needs permissions to the following actions:

* `s3:PutObject`
* `s3:GetObject`
* `s3:DeleteObject`
* `s3:ListBucket`
* `s3:AbortMultipartUpload`

The `ocfl-client` needs to be configured to use a database for locking and caching OCFL object details. This is necessary
to solve eventual consistency and concurrency issues. Use `OcflRepositoryBuilder.objectLock()` and `OcflRepositoryBuilder.objectDetailsDb()`
to set this up. Currently, the only supported databases are PostgreSQL and H2. The `ocfl-java` client populates the object
details database on demand. There is no need to pre-populate it, and it can safely be wiped anytime.

Note, the Amazon S3 storage implementation is not optimized. It will likely not perform well on large files or objects
with lots of files. Additionally, it does not cache any object files locally, requiring them to be retrieved from S3 on
every access. These issues will be addressed in the future.

### Configuration

Use `CloudOcflStorage.builder()` to create and configure an `OcflStorage` instance.

* **cloudClient**: Required, sets the `CloudClient` implementation to use. For Amazon S3, use `OcflS3Client.builder()`.
* **workDir**: Required, sets the location used to stage files as they're moved into and out of the cloud.
* **threadPoolSize**: Optional, sets the size of the storage impls thread pool. By default, the thread pool size is equal
to the number of available processors.

**Example**

```java
var repo = new OcflRepositoryBuilder()
        .layoutConfig(new HashedTruncatedNTupleConfig())
        .contentPathConstraints(ContentPathConstraints.cloud())
        .objectLock(new ObjectLockBuilder().buildDbLock(dataSource))
        .objectDetailsDb(new ObjectDetailsDatabaseBuilder().build(dataSource))
        .storage(CloudOcflStorage.builder()
                .cloudClient(OcflS3Client.builder()
                        .s3Client(s3Client)
                        .bucket(name)
                        .repoPrefix(prefix)
                        .build())
                .workDir(workDir)
                .build())
        .workDir(workDir)
        .build();
```

## Database

If you use a database backed object lock or the object details database, then you'll need to setup a database for the client
to connect to. Currently, only PostgreSQL >= 9.3 and H2 are supported. The client automatically creates the tables that
it needs. If you are managing multiple OCFL repositories, the OCFL tables must be kept in different databases/schemas.

## Usage Considerations

### Running multiple instances

If you intend to write to an OCFL repository from multiple different instances, you should use a database based object lock
rather than the default in-memory lock. This is a requirement for using cloud storage, by not strictly necessary for filesystem
storage. Additionally, you may want to either adjust or disable inventory caching, or hook up a distributed cache implementation.

### Inventory size

OCFL inventory files can grow quite large when an object has lots of files and/or lots of versions. This problem is compounded
by the fact that a copy of the inventory must be persisted in every object version directory. There are three things you can do
to attempt to control inventory bloat:

* Do not generate an excessive number of versions of an object
* Do not pretty print the inventory files (pretty printing is disabled by default)
* Use `sha256` instead of `sha512` for inventory content addressing. `sha512` is the default and the spec recommended algorithm.
On some systems, `sha512` is faster than `sha256`, however, it requires twice as much space to store. If you are concerned about
space, you can change the algorithm by setting `OcflRepositoryBuilder.ocflConfig(new OcflConfig().setDefaultDigestAlgorithm(DigestAlgorithm.sha256))`.
Note, this only changes the digest algorithm used for *new* OCFL objects. It is not possible to modify existing objects.

## APIs

### OcflRepository

See the Javadoc in `OcflRepository` for more detailed information.

* **putObject**: Stores a fully composed object in the repository. The object's previous state is not carried forward. Only
the files that are present in the given path are considered to be part of the new version. However, the files are still dedupped
against previous versions.
* **updateObject**: Unlike `putObject`, `updateObject` carries forward the most recent object state, and allows you to make
one-off changes (adding, removing, moving, etc files) to an object without having the entire object on hand.
* **getObject**: There are two different `getObject` implementations. The first writes a complete copy of an object at a
specified version to a directory outside of the OCFL repository. The second returns an object with lazy-loading references
to all of the files that are part of the specified object version.
* **describeObject**: Returns metadata about an object and all of its versions.
* **describeVersion**: Returns metadata about a specific version of an object.
* **fileChangeHistory**: Returns the change history for a specific file within an object. This is useful for identifying
at what point specific files were changed.
* **containsObject**: Indicates whether the OCFL repository contains an object with the given id.
* **purgeObject**: Permanently removes an object from the repository. The object is NOT recoverable.
* **listObjectIds**: Returns a stream containing the ids of all of the objects in the repository. This API may be slow.
* **close**: Closes the repository, releasing its resources.

### OcflObjectUpdater

See the Javadoc in `OcflObjectUpdater` for more detailed information.

* **addPath**: Adds a file or directory to the object.
* **writeFile**: Adds a file to the object, using an InputStream as the source of the file.
* **removeFile**: Removes the file at the logical path from the object. The file is not removed from storage and can be
reninstated later.
* **renameFile**: Renames a file at an existing logical path to a new logical path.
* **reinstateFile**: Restores a file that existed in a previous version of the object.
* **clearVersionState**: By default, `updateObject` carries forward the current object state, calling `clearVersionState`
clears everything out of the new version, so that it behaves the same as `putObject`.
* **addFileFixity**: Adds an entry to the object's `fixity` block.
* **clearFixityBlock**: Removes all of the entries from the object's `fixity` block.

### OcflOption

A number of the APIs accept optional `OcflOption` arguments.

* **OVERWRITE**: By default, `ocfl-java` will not overwrite files that already exist within an object. If you want to overwrite
a file, you must specify `OcflOption.OVERWRITE` in the operation.
* **MOVE_SOURCE**: By default, `ocfl-java` copies source files into an internal staging directory where it builds the new
object version before moving the version into the repository. Specifying `OcflOption.MOVE_SOURCE` instructs `ocfl-java`
to move the source files into the staging directory instead of copying them.

## Extensions

[OCFL extensions](https://github.com/OCFL/extensions) are additional features that the community has specified that are
outside of the scope of the OCFL spec.

### Storage Layout Extensions

Storage layout extensions describe how OCFL object id should be mapped to paths within the OCFL storage root. `ocfl-java`
includes built-in implementations of these extensions, but, you can override these implementations or add new layout extensions
by writing your own implementations of `OcflStorageLayoutExtension` and registering the extension by calling `OcflExtensionRegistry.register("new-extension-name", NewExtension.class)`
BEFORE initializing your OCFL repository.

The following is a list of currently supported storage layout extensions:

* **0003-hashed-n-tuple-trees**
  * Configuration class: `HashedTruncatedNTupleConfig`
  * Implementation class: `HashedTruncatedNTupleExtension`

### Mutable HEAD Extension

The mutable HEAD extension enables an OCFL object to have a mutable HEAD version that is stored inside of the object root.
This version is NOT an official OCFL version, and it is not recognized by clients that do not implement this extension. This
extension allows you to iteratively make changes to an object without every change producing a new OCFL version. When
you are satisfied with the state of the object, the mutable HEAD version should be committed, which moves it into an official,
immutable OCFL version that is recognized by all OCFL clients.

To enable this extension, call `OcflRepositoryBuilder.buildMutable()`. Note, you do not need to enable the extension for
reading. `ocfl-java` will automatically read mutable HEAD versions that already exist. However, it will not allow you to
write to an object with a mutable HEAD unless the extension is enabled.

#### APIs

See the Javadoc in `MutableOcflRepository` for more detailed information.

* **stageChanges**: This method works the same as `updateObject`, but, instead of making a new version, it updates or creates
a mutable HEAD version.
* **commitStagedChanges**: Converts a mutable HEAD version into an official OCFL version.
* **purgeStagedChanges**: Purges a mutable HEAD version without creating a new OCFL version.
* **hasStagedChanges**: Indicates if an object has a mutable HEAD.
