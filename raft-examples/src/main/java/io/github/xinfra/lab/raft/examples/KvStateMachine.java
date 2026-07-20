/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.examples;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.xinfra.lab.raft.examples.proto.KvCommand;
import io.github.xinfra.lab.raft.examples.proto.KvEntry;
import io.github.xinfra.lab.raft.examples.proto.KvSnapshotData;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public final class KvStateMachine implements AutoCloseable {

    static { RocksDB.loadLibrary(); }

    private final Options options;
    private final RocksDB db;
    private volatile long appliedIndex;

    public KvStateMachine(Path dir) {
        this.options = new Options().setCreateIfMissing(true);
        try {
            Files.createDirectories(dir);
            this.db = RocksDB.open(options, dir.toString());
        } catch (RocksDBException | IOException e) {
            options.close();
            throw new IllegalStateException("failed to open RocksDB at " + dir, e);
        }
    }

    /**
     * Applies a committed command to the store and returns the domain result the
     * proposing node's future should complete with. The concrete type is chosen
     * by the op so the RPC layer receives a strongly typed result with no
     * hand-rolled byte (de)serialization:
     * <ul>
     *   <li>{@code PUT_IF_ABSENT} -> {@link PutIfAbsentResult}</li>
     *   <li>{@code PUT}/{@code DELETE} -> the previous value ({@link String}),
     *       or {@code null} if the key was absent</li>
     * </ul>
     */
    public Object apply(long index, KvCommand cmd) {
        Object result;
        try {
            byte[] keyBytes = bytes(cmd.getKey());
            switch (cmd.getOp()) {
                case PUT -> {
                    result = valueOrNull(db.get(keyBytes));
                    db.put(keyBytes, bytes(cmd.getValue()));
                }
                case PUT_IF_ABSENT -> {
                    byte[] existing = db.get(keyBytes);
                    if (existing == null) {
                        db.put(keyBytes, bytes(cmd.getValue()));
                        result = new PutIfAbsentResult(true, cmd.getValue());
                    } else {
                        result = new PutIfAbsentResult(false, str(existing));
                    }
                }
                case DELETE -> {
                    result = valueOrNull(db.get(keyBytes));
                    db.delete(keyBytes);
                }
                default -> result = null;
            }
        } catch (RocksDBException e) {
            throw new IllegalStateException("apply failed for " + cmd, e);
        }
        appliedIndex = index;
        return result;
    }

    /**
     * Outcome of a {@code PUT_IF_ABSENT}: whether the value was stored, together
     * with the value now associated with the key (the newly stored value when
     * {@code applied} is true, otherwise the pre-existing value).
     */
    public record PutIfAbsentResult(boolean applied, String value) {}

    private static String valueOrNull(byte[] v) {
        return v == null ? null : str(v);
    }

    private static String str(byte[] v) {
        return new String(v, StandardCharsets.UTF_8);
    }

    public Optional<String> get(String key) {
        try {
            byte[] v = db.get(bytes(key));
            return v == null ? Optional.empty() : Optional.of(new String(v, StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            throw new IllegalStateException("get failed for key " + key, e);
        }
    }

    public Map<String, String> dumpAll() {
        Map<String, String> out = new LinkedHashMap<>();
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                out.put(new String(it.key(), StandardCharsets.UTF_8),
                        new String(it.value(), StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    public byte[] serializeState() {
        KvSnapshotData.Builder builder = KvSnapshotData.newBuilder();
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                builder.addEntries(KvEntry.newBuilder()
                        .setKey(new String(it.key(), StandardCharsets.UTF_8))
                        .setValue(new String(it.value(), StandardCharsets.UTF_8)));
            }
        }
        return builder.build().toByteArray();
    }

    public void restoreState(byte[] data) {
        KvSnapshotData snapshot;
        try {
            snapshot = KvSnapshotData.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("failed to parse snapshot data", e);
        }

        try (WriteBatch batch = new WriteBatch();
             WriteOptions wo = new WriteOptions()) {
            try (RocksIterator it = db.newIterator()) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    batch.delete(it.key());
                }
            }
            for (KvEntry entry : snapshot.getEntriesList()) {
                batch.put(bytes(entry.getKey()), bytes(entry.getValue()));
            }
            db.write(wo, batch);
        } catch (RocksDBException e) {
            throw new IllegalStateException("restore failed", e);
        }
    }

    public long appliedIndex() { return appliedIndex; }

    @Override
    public void close() {
        db.close();
        options.close();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
