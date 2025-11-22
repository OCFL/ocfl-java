# OCFL Java

![build](https://github.com/OCFL/ocfl-java/workflows/Build/badge.svg)

This project is a Java implementation of the [OCFL spec](https://ocfl.io).
It supports the following spec versions: 1.0, 1.1.

This project is currently maintained by [Peter
Winckles](https://github.com/pwinckles). Contributions in the form of
Github issues and PRs are welcome, and will be reviewed in a timely
fashion.

## Requirements and Setup

`ocfl-java` requires Java 11 or greater to run.

Add the following to your project's POM to pull in the library:

```xml
<dependency>
    <groupId>io.ocfl</groupId>
    <artifactId>ocfl-java-core</artifactId>
    <version>2.2.3</version>
</dependency>
```

If you want S3 support, you must additionally add the following dependency:

```xml
<dependency>
    <groupId>io.ocfl</groupId>
    <artifactId>ocfl-java-aws</artifactId>
    <version>2.2.3</version>
</dependency>
```

## Example Usage

```java
var repoDir = Paths.get("ocfl-repo"); // This directory contains the OCFL storage root.
var workDir = Paths.get("ocfl-work"); // This directory is used to assemble OCFL versions. It cannot be within the OCFL storage root.

var repo = new OcflRepositoryBuilder()
        .defaultLayoutConfig(new HashedNTupleLayoutConfig())
        .storage(storage -> storage.fileSystem(repoDir))
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

## Additional Documentation

- See the [usage guide](docs/USAGE.md) for more details on how to setup `ocfl-java`
- Javadoc
  - [ocfl-java-api](https://www.javadoc.io/doc/io.ocfl/ocfl-java-api/latest/index.html)
  - [ocfl-java-core](https://www.javadoc.io/doc/io.ocfl/ocfl-java-core/latest/index.html)
  - [ocfl-java-aws](https://www.javadoc.io/doc/io.ocfl/ocfl-java-aws/latest/index.html)
- [Developer guide](docs/DEVELOPMENT.md)
