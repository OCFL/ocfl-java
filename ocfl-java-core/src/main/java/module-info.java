module edu.wisc.library.ocfl.core {

    exports edu.wisc.library.ocfl.core;
    exports edu.wisc.library.ocfl.core.cache;
    exports edu.wisc.library.ocfl.core.concurrent;
    exports edu.wisc.library.ocfl.core.inventory;
    exports edu.wisc.library.ocfl.core.lock;
    exports edu.wisc.library.ocfl.core.mapping;
    exports edu.wisc.library.ocfl.core.model;
    exports edu.wisc.library.ocfl.core.storage;
    // TODO it might be better if this package was not exported
    exports edu.wisc.library.ocfl.core.util;

    requires edu.wisc.library.ocfl.api;
    requires com.fasterxml.jackson.annotation;
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.datatype.jsr310;
    requires com.github.benmanes.caffeine;
    requires org.apache.commons.codec;
    requires org.slf4j;

}