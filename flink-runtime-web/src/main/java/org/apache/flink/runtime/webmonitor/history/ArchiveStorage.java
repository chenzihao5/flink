/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.webmonitor.history;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Abstraction for the storage backend used by the {@link HistoryServer} to read archived job data.
 *
 * <p>Implementations can be backed by the local file system, RocksDB, or any other storage medium.
 *
 * @param <T> Type of the storage entries.
 */
public interface ArchiveStorage<T> extends Closeable {

    /**
     * Returns whether the entry identified by {@code key} exists in this storage.
     *
     * @param key storage key (typically the request path, e.g. {@code /jobs/xxx/config.json})
     * @return {@code true} if the entry exists
     */
    boolean exists(String key);

    /**
     * Returns the entry identified by {@code key} from this storage.
     *
     * @param key storage key
     * @return the entry, or null if the entry does not exist
     * @throws IOException if the entry cannot be read
     */
    @Nullable
    T get(String key) throws IOException;

    /**
     * Stores the entry identified by {@code key} in this storage.
     *
     * @param key storage key
     * @param archiveContent the archive content to store, this type is string because the archive
     *     content is always a JSON String
     * @throws IOException if the entry cannot be written
     */
    void put(String key, String archiveContent) throws IOException;

    /**
     * Deletes the entry identified by {@code key} from this storage.
     *
     * @param key storage key
     * @throws IOException if the entry cannot be deleted
     */
    void delete(String key) throws IOException;

    /**
     * Deletes all entries with key starting with {@code keyPrefix} from this storage.
     *
     * <p>Such as deleting all archived files for a given job or application.
     *
     * @param keyPrefix key prefix
     * @throws IOException if entries cannot be deleted
     */
    void deletePrefix(String keyPrefix) throws IOException;

    /**
     * Returns the entries identified by {@code prefix} from this storage.
     *
     * @param prefix storage key prefix
     * @return the entries
     * @throws IOException if the entries cannot be read
     */
    List<T> getByPrefix(String prefix) throws IOException;
}
