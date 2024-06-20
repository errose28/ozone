/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.checksum;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockMerkleTreeProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerMerkleTreeProto;
import org.apache.hadoop.ozone.common.ChecksumByteBuffer;
import org.apache.hadoop.ozone.common.ChecksumByteBufferFactory;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * This class represents a Merkle tree that provides one checksum for all data within a container.
 *
 * As the leaves of the tree, a checksum for each chunk is computed by taking a checksum of all checksums within that
 * chunk. Each chunk checksum in a block is further checksummed together to generate the block level checksum. Finally,
 * The checksums of all blocks are checksummed together to create a container level checksum.
 * Note that checksums are order dependent. Chunk checksums are sorted by their
 * offset within a block, and block checksums are sorted by their block ID.
 *
 * This class can be used to construct a consistent and completely filled {@link ContainerMerkleTreeProto} object.
 * It allows building a container merkle tree from scratch by incrementally adding chunks.
 * The final checksums at higher levels of the tree are not calculated until
 * {@link ContainerMerkleTree#toProto} is called.
 */
public class ContainerMerkleTree {

  private final SortedMap<Long, BlockMerkleTree> id2Block;

  /**
   * Constructs an empty Container merkle tree object.
   */
  public ContainerMerkleTree() {
    id2Block = new TreeMap<>();
  }

  /**
   * Adds chunks to a block in the tree. The block entry will be created if it is the first time adding chunks to it.
   * If the block entry already exists, the chunks will be added to the existing chunks for that block.
   *
   * @param blockID The ID of the block that these chunks belong to.
   * @param chunks A list of chunks to add to this block. The chunks will be sorted internally by their offset.
   */
  public void addChunks(long blockID, List<ChunkInfo> chunks) {
    id2Block.computeIfAbsent(blockID, BlockMerkleTree::new).addChunks(chunks);
  }

  /**
   * Uses chunk hashes to compute all remaining hashes in the tree, and returns it as a protobuf object. No checksum
   * computation for the tree happens outside of this method.
   *
   * @return A complete protobuf object representation of this tree.
   */
  public ContainerMerkleTreeProto toProto() {
    // Compute checksums and return the result.
    ContainerMerkleTreeProto.Builder containerTreeBuilder = ContainerMerkleTreeProto.newBuilder();
    ChecksumByteBuffer checksumImpl = ChecksumByteBufferFactory.crc32Impl();
    ByteBuffer containerChecksumBuffer = ByteBuffer.allocate(Long.BYTES * id2Block.size());

    for (BlockMerkleTree blockTree: id2Block.values()) {
      // Add block's checksum tree to the proto.
      BlockMerkleTreeProto blockTreeProto = blockTree.toProto();
      containerTreeBuilder.addBlockMerkleTree(blockTreeProto);
      // Add the block's checksum to the buffer to calculate the container checksum.
      containerChecksumBuffer.putLong(blockTreeProto.getBlockChecksum());
    }
    containerChecksumBuffer.flip();
    checksumImpl.update(containerChecksumBuffer);

    return containerTreeBuilder
        .setDataChecksum(checksumImpl.getValue())
        .build();
  }

  /**
   * Represents a merkle tree for a single block within a container.
   */
  private static class BlockMerkleTree {
    // Map of each offset within the block to its chunk info.
    // Chunk order in the checksum is determined by their offset.
    private final SortedMap<Long, ChunkMerkleTree> offset2Chunk;
    private final long blockID;

    BlockMerkleTree(long blockID) {
      this.blockID = blockID;
      this.offset2Chunk = new TreeMap<>();
    }

    /**
     * Adds the specified chunks to this block. The offset value of the chunk must be unique within the block,
     * otherwise it will overwrite the previous value at that offset.
     *
     * @param chunks A list of chunks to add to this block.
     */
    public void addChunks(List<ChunkInfo> chunks) {
      for (ChunkInfo chunk: chunks) {
        offset2Chunk.put(chunk.getOffset(), new ChunkMerkleTree(chunk));
      }
    }

    /**
     * Uses chunk hashes to compute a block hash for this tree, and returns it as a protobuf object. All block checksum
     * computation for the tree happens within this method.
     *
     * @return A complete protobuf object representation of this block tree.
     */
    public BlockMerkleTreeProto toProto() {
      BlockMerkleTreeProto.Builder blockTreeBuilder = BlockMerkleTreeProto.newBuilder();
      ChecksumByteBuffer checksumImpl = ChecksumByteBufferFactory.crc32Impl();
      ByteBuffer blockChecksumBuffer = ByteBuffer.allocate(Long.BYTES * offset2Chunk.size());

      for (ChunkMerkleTree chunkTree: offset2Chunk.values()) {
        // Ordering of checksums within a chunk is assumed to be in the order they are written.
        // This assumption is already built in to the code that reads and writes the values (see
        // ChunkInputStream#validateChunk for an example on the client read path).
        // There is no other value we can use to sort these checksums, so we assume the stored proto has them in the
        // correct order.
        ContainerProtos.ChunkMerkleTreeProto chunkTreeProto = chunkTree.toProto();
        blockTreeBuilder.addChunkMerkleTree(chunkTreeProto);
        blockChecksumBuffer.putLong(chunkTreeProto.getChunkChecksum());
      }
      blockChecksumBuffer.flip();
      checksumImpl.update(blockChecksumBuffer);

      return blockTreeBuilder
          .setBlockID(blockID)
          .setBlockChecksum(checksumImpl.getValue())
          .build();
    }
  }

  /**
   * Represents a merkle tree for a single chunk within a container.
   * Each chunk has multiple checksums within it at each "bytesPerChecksum" interval.
   * This class computes one checksum for the whole chunk by aggregating these.
   */
  private static class ChunkMerkleTree {
    private final ChunkInfo chunk;

    ChunkMerkleTree(ChunkInfo chunk) {
      this.chunk = chunk;
    }

    /**
     * Computes a single hash for this ChunkInfo object. All chunk level checksum computation happens within this
     * method.
     *
     * @return A complete protobuf representation of this chunk as a leaf in the container merkle tree.
     */
    public ContainerProtos.ChunkMerkleTreeProto toProto() {
      ChecksumByteBuffer checksumImpl = ChecksumByteBufferFactory.crc32Impl();
      for (ByteString checksum: chunk.getChecksumData().getChecksums()) {
        checksumImpl.update(checksum.asReadOnlyByteBuffer());
      }

      return ContainerProtos.ChunkMerkleTreeProto.newBuilder()
          .setOffset(chunk.getOffset())
          .setLength(chunk.getLen())
          .setChunkChecksum(checksumImpl.getValue())
          .build();
    }
  }
}
