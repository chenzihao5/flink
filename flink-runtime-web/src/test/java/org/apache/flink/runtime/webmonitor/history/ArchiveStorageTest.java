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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.webmonitor.history;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Basic tests for {@link ArchiveStorage} implementations.
 *
 * <p>Subclasses only need to provide a concrete {@link ArchiveStorage} instance via {@link
 * #createStorage()}. All assertions in this base class go through the {@link ArchiveStorage}
 * interface itself (e.g. {@link ArchiveStorage#exists(String)}), so this test class is independent
 * of any specific storage backend (file system, RocksDB, etc.).
 *
 * @param <T> entry type returned by the storage under test
 */
abstract class ArchiveStorageTest<T> {

    protected ArchiveStorage<T> storage;

    /**
     * Creates {@link ArchiveStorage} instance for a single test.
     *
     * @return storage instance
     */
    protected abstract ArchiveStorage<T> createStorage() throws Exception;

    /** Reads the textual content of a storage entry. */
    protected abstract String readContent(T entry) throws Exception;

    @BeforeEach
    void setUpStorage() throws Exception {
        storage = createStorage();
    }

    // ----------------------------- exists / put / get / delete ----------------------------

    /**
     * Covers the whole lifecycle of a single key across {@code exists}, {@code put}, {@code get}
     * and {@code delete}.
     */
    @Test
    void testBasicLifecycle() throws Exception {
        String key = "/overviews/abc123.json";
        String content = "{\"test\":\"data\"}";

        // exists: missing key
        assertThat(storage.exists(key)).isFalse();

        // put + exists + get returns the same content
        storage.put(key, content);
        assertThat(storage.exists(key)).isTrue();
        assertThat(readContent(storage.get(key))).isEqualTo(content);

        // delete + exists + get returns null
        storage.delete(key);
        assertThat(storage.exists(key)).isFalse();
        assertThat(storage.get(key)).isNull();
    }

    @Test
    void testOverwrite() throws Exception {
        String key = "/overviews/abc123.json";
        String content = "{\"test\":\"data\"}";

        // put + exists + get returns the same content
        storage.put(key, content);
        assertThat(storage.exists(key)).isTrue();
        assertThat(readContent(storage.get(key))).isEqualTo(content);

        // put again: the latest content overwrites the previous one
        String overwriteContent = "{\"test\":\"overwrite_data\"}";
        storage.put(key, overwriteContent);
        assertThat(storage.exists(key)).isTrue();
        assertThat(readContent(storage.get(key))).isEqualTo(overwriteContent);
    }

    // ----------------------------- deletePrefix ----------------------------

    @Test
    void testDeletePrefix() throws Exception {
        String keyUnderPrefix1 = "/jobs/abc123/config.json";
        String keyUnderPrefix2 = "/jobs/abc123/vertices.json";
        String keyOutsidePrefix = "/jobs/def456/config.json";
        storage.put(keyUnderPrefix1, "{\"config\":\"under_prefix_data\"}");
        storage.put(keyUnderPrefix2, "{\"vertices\":\"under_prefix_data\"}");
        storage.put(keyOutsidePrefix, "{\"config\":\"outside_prefix_data\"}");

        storage.deletePrefix("/jobs/abc123");

        assertThat(storage.exists(keyUnderPrefix1)).isFalse();
        assertThat(storage.exists(keyUnderPrefix2)).isFalse();
        assertThat(storage.exists(keyOutsidePrefix)).isTrue();
        assertThat(readContent(storage.get(keyOutsidePrefix)))
                .isEqualTo("{\"config\":\"outside_prefix_data\"}");
    }

    // ----------------------------- getByPrefix ----------------------------

    @Test
    void testGetByPrefix() throws Exception {
        Map<String, String> entriesUnderPrefix = new HashMap<>();
        entriesUnderPrefix.put("/overviews/job1.json", "{\"job\":\"job1\"}");
        entriesUnderPrefix.put("/overviews/job2.json", "{\"job\":\"job2\"}");
        entriesUnderPrefix.put("/overviews/job3.json", "{\"job\":\"job3\"}");
        String keyOutsidePrefix = "/jobs/job1/config.json";

        for (Map.Entry<String, String> e : entriesUnderPrefix.entrySet()) {
            storage.put(e.getKey(), e.getValue());
        }
        storage.put(keyOutsidePrefix, "{\"config\":\"data\"}");

        List<T> result = storage.getByPrefix("/overviews");
        List<String> contents = new ArrayList<>();
        for (T entry : result) {
            contents.add(readContent(entry));
        }
        assertThat(contents).containsExactlyInAnyOrderElementsOf(entriesUnderPrefix.values());
    }
}
