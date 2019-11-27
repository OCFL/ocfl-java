# OCFL Java

This project is a Java implementation of the [OCFL draft spec](https://ocfl.io/draft/spec/).

[![Build Status](https://travis-ci.com/UW-Madison-Library/ocfl-java.svg?branch=master)](https://travis-ci.com/UW-Madison-Library/ocfl-java)

## Requirements and Installation

`ocfl-java` is a Java 11 project and at the minimum requires Java 11 to run.

The `ocfl-java` libraries are not yet being built to a public Maven repository. Until they are, you'll need to build the
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
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## Example Usage

```java
var repoDir = Paths.get("ocfl-repo"); // This directory contains the OCFL storage root.
var workDir = Paths.get("ocfl-work"); // This directory is used to assemble OCFL versions. It cannot be within the OCFL storage root.

var repo = new OcflRepositoryBuilder()
                .layoutConfig(DefaultLayoutConfig.nTupleHashConfig())
                .build(FileSystemOcflStorage.builder().build(repoDir), workDir);

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

## Extension Points

`ocfl-java-core` provides the core framework an OCFL repository, and exposes a number of extension points for configuring
its behavior and storage layer. The core implementation class is `edu.wisc.library.ocfl.core.DefaultOcflRepository`.

### Storage

Storage layer implementations must implement `edu.wisc.library.ocfl.core.OcflStorage`. There are the following existing
implementations:

* `FileSystemOcflStorage`: Basic implementation that stores all objects within a single root on the local filesystem.

### Object ID Mapping

Object IDs must be mapped to paths relative to the OCFL root. The OCFL spec does not define specifically how this should
be done. Out of the box, `ocfl-java` supports three different mapping algorithms, and additional implementations can be
defined by implementing `edu.wisc.library.oclf.core.mapping.ObjectIdPathMapper`.

* `FlatObjectIdPathMapper`: Encodes object IDs using `UrlEncoder` or `PairTreeEncoder`. The encoded ID is used to create
an object root as a direct child of the OCFL root. This is the simplest mapper, but has serious performance problems and
should generally be avoided.
* `NTupleObjectIdPathMapper`: Supports [pairtree](https://tools.ietf.org/html/draft-kunze-pairtree-01) mapping as well as
truncated n-tuple. It performs much better than `FlatObjectIdPathMapper`. Using an encoder that hashes the object ID is
recommended, so that the storage directory tree is balanced.

### Locking

A lock is used to guard against concurrent object edits. Custom lock implementations can be defined by implementing
`edu.wisc.library.ocfl.core.lock.ObjectLock`. The following implementations are provided:

* `InMemoryObjectLock`: Simple in-memory lock implementation that's based on Java's `ReentrantReadWriteLock`. This implementation
is suitable when a single application is operating on an OCFL repository.

### Caching

A cache is used so that an object's inventory does not need to parsed on every request. Custom cache implementations should
implement `edu.wisc.library.ocfl.core.cache.Cache`. The following implementations are provided:

* `NoOpCache`: Use this if you never want to cache anything.
* `CaffeineCache`: In-memory cache that uses [Caffeine](https://github.com/ben-manes/caffeine).
