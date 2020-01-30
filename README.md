# OCFL Java

This project is a work-in-progress Java implementation of the [OCFL draft spec](https://ocfl.io/draft/spec/).

**Disclaimer**: This library is under active development, and the OCFL specification has not been finalized. I will be making
breaking changes up until a 1.0.0 release. I do not recommend using this library in a production setting until then.

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

After building the libraries locally, add the following to you're project's POM:

```xml
<dependency>
    <groupId>edu.wisc.library.ocfl</groupId>
    <artifactId>ocfl-java-core</artifactId>
    <version>0.0.2-SNAPSHOT</version>
</dependency>
```

Add the following if you'd like to use Amazon S3 for the storage layer:

```xml
<dependency>
    <groupId>edu.wisc.library.ocfl</groupId>
    <artifactId>ocfl-java-aws</artifactId>
    <version>0.0.2-SNAPSHOT</version>
</dependency>
```

## Example Usage

```java
var repoDir = Paths.get("ocfl-repo"); // This directory contains the OCFL storage root.
var workDir = Paths.get("ocfl-work"); // This directory is used to assemble OCFL versions. It cannot be within the OCFL storage root.

var repo = new OcflRepositoryBuilder()
                .layoutConfig(DefaultLayoutConfig.nTupleHashConfig())
                .storage(FileSystemOcflStorage.builder().repositoryRoot(repoDir).build())
                .workDir(workDir)
                .build();

repo.putObject(ObjectVersionId.head("o1"), Paths.get("object-out-dir"), new CommitInfo().setMessage("initial commit"));
repo.getObject(ObjectVersionId.head("o1"), Paths.get("object-in-dir"));

repo.updateObject(ObjectVersionId.head("o1"), new CommitInfo().setMessage("update"), updater -> {
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
not required when opening an existing repository. Use `DefaultLayoutConfig` for preconfigured layout options, or build your
own using `FlatLayoutConfig` or `NTupleLayoutConfig`. The recommended layout is `DefaultLayoutConfig.nTupleHashConfig()`.

### Optional Properties

* **ocflConfig**: Sets the following default values that are used for creating new OCFL objects: version, digest algorithm,
and content directory. The defaults are `v1`, `sha512`, and `content`.
* **prettyPrintJson**: Enables pretty print JSON in newly written inventory files. By default, all extranious whitespace
is removed to reduce the file size.
* **contentPathConstraintProcessor**: Configures what file name constraints are enforced on OCFL content paths. By default,
there are no special constraints applied. Used `DefaultContentPathConstraints` for a selection of preconfigured defaults.
You may want to apply constraints if you are concerned about portablility between filesystems.
* **pathSanitizer**: `PathSanitizer` implementations are used to map logical paths to safe content paths. By default, no
special mapping is done and logical paths are mapped directly to content paths.
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

## APIs