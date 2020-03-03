module edu.wisc.library.ocfl.core {

    exports edu.wisc.library.ocfl.core;
    exports edu.wisc.library.ocfl.core.cache;
    exports edu.wisc.library.ocfl.core.concurrent;
    exports edu.wisc.library.ocfl.core.db;
    exports edu.wisc.library.ocfl.core.encode;
    exports edu.wisc.library.ocfl.core.extension.layout;
    exports edu.wisc.library.ocfl.core.extension.layout.config;
    exports edu.wisc.library.ocfl.core.inventory;
    exports edu.wisc.library.ocfl.core.lock;
    exports edu.wisc.library.ocfl.core.mapping;
    exports edu.wisc.library.ocfl.core.model;
    exports edu.wisc.library.ocfl.core.path;
    exports edu.wisc.library.ocfl.core.path.constraint;
    exports edu.wisc.library.ocfl.core.path.sanitize;
    exports edu.wisc.library.ocfl.core.storage;
    exports edu.wisc.library.ocfl.core.storage.cloud;
    exports edu.wisc.library.ocfl.core.storage.filesystem;
    exports edu.wisc.library.ocfl.core.util;

    requires bytes;
    requires com.fasterxml.jackson.annotation;
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.datatype.jsr310;
    requires com.github.benmanes.caffeine;
    requires com.google.common;
    requires edu.wisc.library.ocfl.api;
    requires failsafe;
    requires java.naming;
    requires java.sql;
    requires org.slf4j;

}