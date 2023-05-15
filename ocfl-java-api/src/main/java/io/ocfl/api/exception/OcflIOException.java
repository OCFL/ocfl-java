/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 University of Wisconsin Board of Regents
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package io.ocfl.api.exception;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;

/**
 * This exception is a wrapper around an IOException
 */
public class OcflIOException extends OcflJavaException {

    private final Exception cause;
    private final boolean hasMessage;

    /**
     * Wraps an IO exception in the appropriate wrapper class. For example, NoSuchFileException to OcflNoSuchFileException.
     *
     * @param e the base exception
     * @return wrapped exception
     */
    public static OcflIOException from(IOException e) {
        if (e instanceof NoSuchFileException || e instanceof FileNotFoundException) {
            return new OcflNoSuchFileException(e);
        } else if (e instanceof FileAlreadyExistsException) {
            return new OcflFileAlreadyExistsException(e);
        }

        return new OcflIOException(e);
    }

    public OcflIOException(Exception cause) {
        super(cause);
        this.cause = cause;
        this.hasMessage = false;
    }

    public OcflIOException(String message, Exception cause) {
        super(message, cause);
        this.cause = cause;
        this.hasMessage = true;
    }

    public OcflIOException(String message) {
        super(message);
        this.cause = null;
        this.hasMessage = true;
    }

    @Override
    public String getMessage() {
        if (hasMessage || cause == null) {
            return super.getMessage();
        }
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }
}
