/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.xinfra.lab.raft.transport.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.xinfra.lab.raft.Transport;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.raft.transport.grpc.proto.Ack;
import io.github.xinfra.lab.raft.transport.grpc.proto.RaftMessage;
import io.github.xinfra.lab.raft.transport.grpc.proto.RaftTransportServiceGrpc;
import io.github.xinfra.lab.raft.transport.grpc.proto.SnapshotChunk;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.function.Supplier;

/** Server side of {@link GrpcTransport}. */
final class RaftServiceImpl extends RaftTransportServiceGrpc.RaftTransportServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(RaftServiceImpl.class);

    private final long localId;
    private final Supplier<Transport.MessageReceiver> receiverSupplier;

    RaftServiceImpl(long localId, Supplier<Transport.MessageReceiver> receiverSupplier) {
        this.localId = localId;
        this.receiverSupplier = receiverSupplier;
    }

    @Override
    public void send(RaftMessage request, StreamObserver<Ack> responseObserver) {
        if (request.getTo() != localId) {
            responseObserver.onNext(Ack.newBuilder().setOk(false)
                    .setError("misrouted: to=" + request.getTo() + " local=" + localId).build());
            responseObserver.onCompleted();
            return;
        }
        Transport.MessageReceiver r = receiverSupplier.get();
        if (r == null) {
            responseObserver.onNext(Ack.newBuilder().setOk(false).setError("receiver not ready").build());
            responseObserver.onCompleted();
            return;
        }
        try {
            Eraftpb.Message msg = Eraftpb.Message.parseFrom(request.getPayload());
            r.receive(msg);
            responseObserver.onNext(Ack.newBuilder().setOk(true).build());
            responseObserver.onCompleted();
        } catch (InvalidProtocolBufferException e) {
            LOG.warn("dropping malformed Eraftpb.Message from {}: {}", request.getFrom(), e.toString());
            responseObserver.onNext(Ack.newBuilder().setOk(false).setError("malformed payload").build());
            responseObserver.onCompleted();
        } catch (Throwable t) {
            LOG.error("receiver threw on inbound from {}: {}", request.getFrom(), t.toString());
            responseObserver.onNext(Ack.newBuilder().setOk(false).setError("receiver error").build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public StreamObserver<SnapshotChunk> installSnapshot(StreamObserver<Ack> responseObserver) {
        // Reassemble: chunk[0].payload starts with [4-byte BE envelope length][envelope bytes][slice of snapData];
        // subsequent chunks are pure snapData slices.
        return new StreamObserver<>() {
            final ByteArrayOutputStream buf = new ByteArrayOutputStream();
            long expectedFrom = -1;
            long expectedTotal = -1;

            @Override
            public void onNext(SnapshotChunk chunk) {
                if (chunk.getTo() != localId) {
                    responseObserver.onError(new IllegalArgumentException(
                            "snapshot misrouted: to=" + chunk.getTo() + " local=" + localId));
                    return;
                }
                if (expectedFrom == -1) {
                    expectedFrom = chunk.getFrom();
                    expectedTotal = chunk.getTotalBytes();
                } else if (chunk.getFrom() != expectedFrom) {
                    responseObserver.onError(new IllegalArgumentException(
                            "snapshot stream interleaved from=" + chunk.getFrom() + " expected=" + expectedFrom));
                    return;
                }
                try {
                    chunk.getPayload().writeTo(buf);
                } catch (java.io.IOException e) {
                    responseObserver.onError(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                LOG.warn("snapshot stream from {} aborted: {}", expectedFrom, t.toString());
            }

            @Override
            public void onCompleted() {
                try {
                    byte[] all = buf.toByteArray();
                    if (all.length < 4) {
                        responseObserver.onError(new IllegalStateException("snapshot stream truncated"));
                        return;
                    }
                    int envLen = ((all[0] & 0xff) << 24)
                            | ((all[1] & 0xff) << 16)
                            | ((all[2] & 0xff) << 8)
                            | (all[3] & 0xff);
                    if (envLen < 0 || envLen + 4 > all.length) {
                        responseObserver.onError(new IllegalStateException("snapshot envelope length invalid"));
                        return;
                    }
                    byte[] envBytes = new byte[envLen];
                    System.arraycopy(all, 4, envBytes, 0, envLen);
                    Eraftpb.Message envelope = Eraftpb.Message.parseFrom(envBytes);

                    int dataStart = 4 + envLen;
                    int dataLen = all.length - dataStart;

                    Eraftpb.Snapshot reassembled = envelope.getSnapshot().toBuilder()
                            .setData(ByteString.copyFrom(all, dataStart, dataLen))
                            .build();
                    Eraftpb.Message full = envelope.toBuilder()
                            .setSnapshot(reassembled)
                            .build();

                    Transport.MessageReceiver r = receiverSupplier.get();
                    if (r == null) {
                        responseObserver.onNext(Ack.newBuilder().setOk(false).setError("receiver not ready").build());
                    } else {
                        r.receive(full);
                        responseObserver.onNext(Ack.newBuilder().setOk(true).build());
                    }
                    responseObserver.onCompleted();
                } catch (InvalidProtocolBufferException e) {
                    responseObserver.onError(e);
                }
            }
        };
    }
}
