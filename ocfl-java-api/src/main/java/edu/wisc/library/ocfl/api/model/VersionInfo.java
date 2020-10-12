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

package edu.wisc.library.ocfl.api.model;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.time.OffsetDateTime;
import java.util.Objects;

/**
 * Descriptive information about an object version.
 */
public class VersionInfo {

    private User user;
    private String message;
    private OffsetDateTime created;

    public VersionInfo() {
        this.user = new User();
    }

    /**
     * The user who authored the version
     *
     * @return user object
     */
    public User getUser() {
        return user;
    }

    /**
     * Sets the user info
     *
     * @param name the user's name, required
     * @param address a URI that identifies the user, such as email address
     * @return this
     */
    public VersionInfo setUser(String name, String address) {
        user.setName(Enforce.notBlank(name, "username cannot be blank"))
                .setAddress(address);
        return this;
    }

    /**
     * Description of version changes
     *
     * @return the version description
     */
    public String getMessage() {
        return message;
    }

    /**
     * Sets the version description
     *
     * @param message version description
     * @return this
     */
    public VersionInfo setMessage(String message) {
        this.message = message;
        return this;
    }

    /**
     * The timestamp when the version was created
     *
     * @return version creation timestamp
     */
    public OffsetDateTime getCreated() {
        return created;
    }

    /**
     * Sets the verison creation timestamp. If this value is not supplied, the current system time will be used instead.
     *
     * @param created version creation timestamp
     * @return this
     */
    public VersionInfo setCreated(OffsetDateTime created) {
        this.created = created;
        return this;
    }

    @Override
    public String toString() {
        return "VersionInfo{" +
                "user=" + user +
                ", message='" + message + '\'' +
                ", created=" + created +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VersionInfo that = (VersionInfo) o;
        return user.equals(that.user) &&
                Objects.equals(message, that.message) &&
                Objects.equals(created, that.created);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, message, created);
    }

}
