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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompressionType;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link ArchiveStorage} implementation backed by RocksDB.
 *
 * <p>All archived data is stored as key-value pairs in a single RocksDB instance, avoiding the
 * problem of numerous small files. The key is the request path (e.g. {@code
 * /jobs/xxx/config.json}), and the value is a JSON string.
 */
public class RocksDBArchiveStorage implements ArchiveStorage<String> {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBArchiveStorage.class);

    private final RocksDB db;

    private BlockBasedTableConfig tableFormatConfig;

    private Options dbOptions;

    private WriteOptions writeOptions;

    private BloomFilter bloomFilter;

    private final ArrayList<AutoCloseable> handlesToClose;

    /**
     * Creates a new {@link RocksDBArchiveStorage} instance with default RocksDB options.
     *
     * @param dbPath the RocksDB database directory path
     * @throws IOException if the RocksDB database cannot be opened
     */
    public RocksDBArchiveStorage(File dbPath) throws IOException {
        this(dbPath, new Configuration());
    }

    /**
     * Creates a new {@link RocksDBArchiveStorage} instance.
     *
     * @param dbPath the RocksDB database directory path
     * @param config the configuration used to read RocksDB related options (see {@link
     *     HistoryServerOptions})
     * @throws IOException if the RocksDB native library cannot be loaded or the database cannot be
     *     opened
     */
    public RocksDBArchiveStorage(File dbPath, ReadableConfig config) throws IOException {
        checkNotNull(dbPath, "dbPath");
        checkNotNull(config, "config");
        this.handlesToClose = new ArrayList<>();
        String rocksDBNativeLibDir =
                config.getOptional(
                                HistoryServerOptions.HISTORY_SERVER_ARCHIVE_ROCKSDB_NATIVE_LIB_DIR)
                        .orElseGet(() -> System.getProperty("java.io.tmpdir"));

        try {
            loadRocksDBLibrary(rocksDBNativeLibDir);
            loadConfiguration(config);
            this.db = RocksDB.open(dbOptions, dbPath.getAbsolutePath());
            handlesToClose.add(db);
        } catch (Throwable t) {
            throw new IOException("Failed to initialize RocksDBArchiveStorage", t);
        }
    }

    @Override
    public boolean exists(String key) {
        try {
            return db.get(key.getBytes(UTF_8)) != null;
        } catch (RocksDBException e) {
            LOG.warn("Failed to check existence of key: {}", key, e);
            return false;
        }
    }

    @Nullable
    @Override
    public String get(String key) throws IOException {
        try {
            byte[] value = db.get(key.getBytes(UTF_8));
            if (value == null) {
                return null;
            }
            return new String(value, UTF_8);
        } catch (RocksDBException e) {
            throw new IOException("Failed to get key: " + key, e);
        }
    }

    @Override
    public void put(String key, String value) throws IOException {
        try {
            db.put(writeOptions, key.getBytes(UTF_8), value.getBytes(UTF_8));
        } catch (RocksDBException e) {
            throw new IOException("Failed to put key: " + key, e);
        }
    }

    @Override
    public void delete(String key) throws IOException {
        try {
            db.delete(writeOptions, key.getBytes(UTF_8));
        } catch (RocksDBException e) {
            throw new IOException("Failed to delete key: " + key, e);
        }
    }

    @Override
    public void deletePrefix(String keyPrefix) throws IOException {
        if (keyPrefix == null || keyPrefix.isEmpty()) {
            return;
        }

        try {
            // Delete all keys that start with the given prefix
            byte[] startKey = keyPrefix.getBytes(UTF_8);
            byte[] endKey = keyPrefix.getBytes(UTF_8);
            // Add 1 to the last byte to get the next lexicographic byte
            endKey[endKey.length - 1]++;
            db.deleteRange(writeOptions, startKey, endKey);
        } catch (RocksDBException e) {
            throw new IOException("Failed to delete prefix: " + keyPrefix, e);
        }
    }

    @Override
    public List<String> getByPrefix(String prefix) throws IOException {
        List<String> result = new ArrayList<>();
        if (prefix == null || prefix.isEmpty()) {
            return result;
        }

        try (RocksIterator iterator = db.newIterator()) {
            byte[] prefixBytes = prefix.getBytes(UTF_8);
            iterator.seek(prefixBytes);
            while (iterator.isValid()) {
                byte[] keyBytes = iterator.key();
                byte[] valueBytes = iterator.value();
                String currentKey = new String(keyBytes, UTF_8);
                String currentValue = new String(valueBytes, UTF_8);

                if (currentKey.startsWith(prefix)) {
                    result.add(currentValue);
                    iterator.next();
                } else {
                    break;
                }
            }
        }

        return result;
    }

    @Override
    public void close() {
        handlesToClose.forEach(IOUtils::closeQuietly);
        handlesToClose.clear();
    }

    /**
     * Loads the RocksDB native library. The default configuration is suitable for most use cases.
     * So we only expose a few options.
     */
    private void loadConfiguration(ReadableConfig config) {

        // Use the full (non-block-based) BloomFilter format with 10 bits/key, which is the
        // RocksDB-recommended default and yields ~1% false positive rate.
        // https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter#full-filters-new-format
        this.bloomFilter = new BloomFilter(10.0D, false);
        handlesToClose.add(bloomFilter);

        this.tableFormatConfig =
                new BlockBasedTableConfig()
                        .setFilterPolicy(bloomFilter)
                        .setEnableIndexCompression(false)
                        .setIndexBlockRestartInterval(8)
                        .setFormatVersion(5);

        this.dbOptions =
                new Options()
                        .setCreateIfMissing(true)
                        .setBottommostCompressionType(
                                CompressionType.valueOf(
                                        config.get(
                                                HistoryServerOptions
                                                        .HISTORY_SERVER_ARCHIVE_ROCKSDB_BOTTOMMOST_COMPRESSION)))
                        .setCompressionType(
                                CompressionType.valueOf(
                                        config.get(
                                                HistoryServerOptions
                                                        .HISTORY_SERVER_ARCHIVE_ROCKSDB_COMPRESSION)))
                        .setTableFormatConfig(tableFormatConfig);
        handlesToClose.add(dbOptions);

        this.writeOptions = new WriteOptions().setDisableWAL(true);
        handlesToClose.add(writeOptions);
    }

    private static void loadRocksDBLibrary(String baseTempDir) throws IOException {
        File baseDir = new File(checkNotNull(baseTempDir, "baseTempDir"));
        File libDir = new File(baseDir, "flink-history-rocksdb-lib-" + UUID.randomUUID());
        try {
            Files.createDirectories(libDir.toPath());
            LOG.info("Try to load RocksDB native library from '{}'.", libDir.getAbsolutePath());
            NativeLibraryLoader.getInstance().loadLibrary(libDir.getAbsolutePath());
            // Sanity check that the library is fully usable.
            RocksDB.loadLibrary();
            LOG.info(
                    "Successfully loaded RocksDB native library from '{}'.",
                    libDir.getAbsolutePath());
        } catch (Throwable t) {
            LOG.warn(
                    "Failed to load RocksDB native library from '{}'.",
                    libDir.getAbsolutePath(),
                    t);
            deleteDirectoryQuietly(libDir);
            throw t;
        }
    }

    private static void deleteDirectoryQuietly(File dir) {
        try {
            FileUtils.deleteDirectory(dir);
        } catch (Throwable t) {
            LOG.warn("Failed to delete directory: {}", dir.getAbsolutePath(), t);
        }
    }
}
