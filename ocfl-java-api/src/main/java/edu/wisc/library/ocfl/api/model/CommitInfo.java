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

/**
 * Descriptive information about an object version.
 */
public class CommitInfo {

    private User user;
    private String message;

    /**
     * Convenience method for constructing a commit info object.
     *
     * @param message the commit message
     * @param userName the name of the user who committed the change
     * @param userAddress the address of the user who committed the change
     * @return commit info
     */
    public static CommitInfo build(String message, String userName, String userAddress) {
        var info = new CommitInfo().setMessage(message);
        if (userName != null || userAddress != null) {
            info.setUser(new User()
                    .setName(userName)
                    .setAddress(userAddress));
        }
        return info;
    }

    /**
     * The user who authored the version
     */
    public User getUser() {
        return user;
    }

    public CommitInfo setUser(User user) {
        this.user = user;
        return this;
    }

    /**
     * Description of version changes
     */
    public String getMessage() {
        return message;
    }

    public CommitInfo setMessage(String message) {
        this.message = message;
        return this;
    }

    @Override
    public String toString() {
        return "CommitInfo{" +
                "user=" + user +
                ", message='" + message + '\'' +
                '}';
    }

}
