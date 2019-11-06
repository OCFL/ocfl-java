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

var repo = new OcflRepositoryBuilder().build(
        new FileSystemOcflStorageBuilder().build(repoDir, new ObjectIdPathMapperBuilder()
                .withDefaultCaffeineCache().buildDefaultPairTreeMapper()),
        workDir);

repo.putObject(ObjectId.head("o1"), Paths.get("object-out-dir"), new CommitInfo().setMessage("initial commit"));
repo.getObject(ObjectId.head("o1"), Paths.get("object-in-dir"));

repo.updateObject(ObjectId.head("o1"), new CommitInfo().setMessage("update"), updater -> {
    updater.addPath(Paths.get("path-to-file2"), "file2")
            .removeFile("file1")
            .addPath(Paths.get("path-to-file3"), "dir1/file3");
});

repo.readObject(ObjectId.version("o1", "v1"), reader -> {
    reader.listFiles();
    reader.getFile("path", Paths.get("destination"));
});
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
* `PairTreeObjectIdPathMapper`: This is an implementation the [pairtree](https://tools.ietf.org/html/draft-kunze-pairtree-01)
spec. It sports much better performance than `FlatObjectIdPathMapper`. It's draw back is that it produces deep, unbalanced
trees.
* `HashingObjectIdPathMapper`: Similar to `PairTreeObjectIdPathMapper` except that object IDs are hashed and the depth
is truncated. This is the most performant option because the file tree is shallow and balanced. It's disadvantage, compared
to `PairTreeObjectIdPathMapper`, is that directory names are not reversible to object IDs.
* `CachingObjectIdPathMapper`: This mapper can be used to wrap any `ObjectIdPathMapper` and cache its results.

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
