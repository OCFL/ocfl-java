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

package edu.wisc.library.ocfl.api.exception;

/**
 * This exception is a wrapper around FileAlreadyExistsException
 */
public class OcflFileAlreadyExistsException extends OcflIOException {

    private final Exception cause;
    private final boolean hasMessage;

    public OcflFileAlreadyExistsException(Exception cause) {
        super(cause);
        this.cause = cause;
        this.hasMessage = false;
    }

    public OcflFileAlreadyExistsException(String message, Exception cause) {
        super(message, cause);
        this.cause = cause;
        this.hasMessage = true;
    }

    @Override
    public String getMessage() {
        if (hasMessage) {
            return super.getMessage();
        }
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }

}
