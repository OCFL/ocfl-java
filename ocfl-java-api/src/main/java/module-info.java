module edu.wisc.library.ocfl.api {

    exports edu.wisc.library.ocfl.api;
    exports edu.wisc.library.ocfl.api.exception;
    exports edu.wisc.library.ocfl.api.io;
    exports edu.wisc.library.ocfl.api.model;
    exports edu.wisc.library.ocfl.api.util;

    requires com.fasterxml.jackson.annotation;
    requires org.apache.commons.codec;

}