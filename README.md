# OCFL Java Core

This project is an implementation of the [ocfl-java-api](https://github.com/pwinckles/ocfl-java-api) and conforms to the
[OCFL beta 0.3 spec](https://ocfl.io/0.3/spec/). It does not implement a storage layer.

## Extension Points

`ocfl-java-core` provides the core framework an OCFL repository, and exposes a number of extension points for configuring
its behavior and storage layer. The core implementation class is `edu.wisc.library.ocfl.core.DefaultOcflRepository`.

### Storage

Storage layer implementations must implement `edu.wisc.library.ocfl.core.OcflStorage`. There are the following existing
implementations:

* [ocfl-java-filesystem](https://github.com/pwinckles/ocfl-java-filesystem)

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