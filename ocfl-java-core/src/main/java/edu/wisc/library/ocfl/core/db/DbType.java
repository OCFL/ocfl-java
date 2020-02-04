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

package edu.wisc.library.ocfl.core.db;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * This enum describes the database types that the library supports out of the box.
 */
public enum DbType {

    POSTGRES("PostgreSQL"),
    H2("H2");

    private String productName;

    DbType(String productName) {
        this.productName = productName;
    }

    public static DbType fromDataSource(DataSource dataSource) {
        try (var connection = dataSource.getConnection()) {
            var productName = connection.getMetaData().getDatabaseProductName();

            for (var type : values()) {
                if (type.productName.equals(productName)) {
                    return type;
                }
            }

            throw new IllegalArgumentException(String.format("%s is not mapped to a DbType.", productName));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
