/*
 * Copyright (c) 1995, 2017, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package edu.wisc.library.ocfl.core.encode;

import com.google.common.escape.Escaper;

/**
 * URL encodes a string.
 */
public class UrlEncoder implements Encoder {

    private static final String SAFE_CHARS = "-_.*";

    private Escaper escaper;

    /**
     * @param useUppercase true if hex digits should be upper case
     */
    public UrlEncoder(boolean useUppercase) {
        this.escaper = PercentEscaper.builder()
                .safeChars(SAFE_CHARS)
                .useUppercase(useUppercase)
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String encode(String input) {
        return escaper.escape(input);
    }

}
