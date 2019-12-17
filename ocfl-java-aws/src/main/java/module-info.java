module edu.wisc.library.ocfl.aws {

    exports edu.wisc.library.ocfl.aws;

    requires bytes;
    requires edu.wisc.library.ocfl.api;
    requires edu.wisc.library.ocfl.core;
    requires org.reactivestreams;
    requires org.slf4j;
    requires software.amazon.awssdk.core;
    requires software.amazon.awssdk.awscore;
    requires software.amazon.awssdk.services.s3;
    requires software.amazon.awssdk.utils;

}