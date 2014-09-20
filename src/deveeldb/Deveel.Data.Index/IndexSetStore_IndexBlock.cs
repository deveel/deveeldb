// 
//  Copyright 2010-2014 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Collections.Generic;

using Deveel.Data.Store;
using Deveel.Data.Util;

namespace Deveel.Data.Index {
	sealed partial class IndexSetStore {
		/// <summary>
		/// Represents a single 'Index block' area in the store.
		/// </summary>
		/// <remarks>
		/// <para>
		/// <list type="table">
		///	   <listheader>
		///    An index block area contains an entry for each index element in an index. 
		///    Each entry is 28 bytes in size and the area has a 16 byte header.
		///    </listheader>
		///    <item>
		///        <term><b>Header</b></term>
		///        <description>( version (int), reserved (int), index size (long) )</description>
		///     </item>
		///     <item>
		///         <term><b>Entry</b></term>
		///         <description>( first entry (long), last entry (long),
		///         index element pointer (long), type/element size (int) )</description>
		///     </item>
		/// </list>
		/// </para>
		/// <para>
		/// type/element size contains the number of elements in the block, and the block 
		/// compaction factor. For example, type 1 means the block contains short sized index 
		/// values, 2 is int sized index values, and 3 is long sized index values.
		/// </para>
		/// </remarks>
		class IndexBlock {
			private readonly IndexSetStore store;

			/// <summary>
			/// The number of references to this object.
			/// </summary>
			/// <remarks>
			/// When this reaches 0, it is safe to free any resources that 
			/// this block deleted.
			/// </remarks>
			private int referenceCount;

			/// <summary>
			/// The index of this block in the index set.
			/// </summary>
			private readonly int indexNum;

			/// <summary>
			/// A pointer that references the area in the store.
			/// </summary>
			private readonly long indexBlockPointer;

			/// <summary>
			/// The total number of entries in the index block.
			/// </summary>
			private readonly long blockEntries;

			/// <summary>
			/// The block size of elements in this block.
			/// </summary>
			private readonly int blockSize;

			/// <summary>
			/// The list of deleted areas that can safely be disposed when 
			/// this object is disposed.
			/// </summary>
			private List<long> deletedAreas;

			/// <summary>
			/// True if this block is marked as deleted.
			/// </summary>
			private bool deleted;

			/// <summary>
			/// Set to true when this index block is freed from the index store.
			/// </summary>
			private bool freed;

			/// <summary>
			/// The parent IndexBlock.  This block is a child modification of the parent.
			/// </summary>
			private IndexBlock parentBlock;

			/// <summary>
			/// Constructs the IndexBlock.
			/// </summary>
			/// <param name="store"></param>
			/// <param name="indexNum"></param>
			/// <param name="blockSize"></param>
			/// <param name="indexBlockPointer"></param>
			public IndexBlock(IndexSetStore store, int indexNum, int blockSize, long indexBlockPointer) {
				this.store = store;
				this.indexNum = indexNum;
				this.blockSize = blockSize;
				this.indexBlockPointer = indexBlockPointer;

				// Read the index count
				IArea indexBlockArea = store.store.GetArea(indexBlockPointer);
				indexBlockArea.Position = 8;
				blockEntries = indexBlockArea.ReadInt8();

				referenceCount = 0;
			}

			/// <summary>
			/// Sets the parent IndexBlock, the index that this index block succeeded.
			/// </summary>
			/// <param name="parent"></param>
			public void SetParentIndexBlock(IndexBlock parent) {
				parentBlock = parent;
			}

			/// <summary>
			/// Returns a list of pointers to all mapped blocks.
			/// </summary>
			public long[] GetBlockPointers() {
				// Create an area for the index block pointer
				IArea indexBlockArea = store.store.GetArea(indexBlockPointer);

				// First create the list of block entries for this list      
				long[] blocks = new long[(int) blockEntries];
				if (blockEntries != 0) {
					indexBlockArea.Position = 16;
					for (int i = 0; i < blockEntries; ++i) {
						// NOTE: We cast to 'int' here because of internal limitations.
						indexBlockArea.ReadInt8();
						indexBlockArea.ReadInt8();
						long elementP = indexBlockArea.ReadInt8();
						indexBlockArea.ReadInt4();

						blocks[i] = elementP;
					}
				}

				return blocks;
			}

			/// <summary>
			/// Creates and returns an array of all the <see cref="IMappedBlock"/> objects 
			/// that make up this view of the index integer list.
			/// </summary>
			/// <returns></returns>
			private IMappedBlock[] CreateMappedBlocks() {
				// Create an area for the index block pointer
				IArea indexBlockArea = store.store.GetArea(indexBlockPointer);

				// First create the list of block entries for this list      
				IMappedBlock[] blocks = new IMappedBlock[(int)blockEntries];
				if (blockEntries != 0) {
					indexBlockArea.Position = 16;
					for (int i = 0; i < blockEntries; ++i) {
						// NOTE: We cast to 'int' here because of internal limitations.
						long firstEntry = indexBlockArea.ReadInt8();
						long lastEntry = indexBlockArea.ReadInt8();
						long blockPointer = indexBlockArea.ReadInt8();
						int typeSize = indexBlockArea.ReadInt4();

						//TODO: check this...
						// size is the first 24 bits (max size = 16MB)
						int elementCount = typeSize & 0x0FFF;
						byte type = (byte)(ByteBuffer.URShift(typeSize, 24) & 0x0F);

						blocks[i] = Index.NewMappedListBlock(store, firstEntry, lastEntry, blockPointer, elementCount, type, blockSize);
					}
				}

				return blocks;
			}

			/// <summary>
			/// Creates and returns a mutable Index object based on this
			/// view of the index.
			/// </summary>
			/// <returns></returns>
			public IIndex CreateIndex() {
				// Create the MappedListBlock objects for this view
				IMappedBlock[] blocks = CreateMappedBlocks();
				// And return the Index
				return new Index(store, indexNum, blockSize, blocks);
			}

			/// <summary>
			/// Copies this index block to the given Store and returns a pointer to 
			/// the block within the store.
			/// </summary>
			/// <param name="destStore"></param>
			/// <returns></returns>
			public long CopyTo(IStore destStore) {
				// Create the MappedListBlock object list for this view
				IMappedBlock[] blocks = CreateMappedBlocks();
				try {
					destStore.LockForWrite();
					// Create the header area in the store for this block
					IAreaWriter a = destStore.CreateArea(16 + (blocks.Length * 28));
					long blockP = a.Id;

					a.WriteInt4(1);               // version
					a.WriteInt4(0);               // reserved
					a.WriteInt8(blocks.Length);  // block count
					for (int i = 0; i < blocks.Length; ++i) {
						IMappedBlock entry = blocks[i];
						long b_p = entry.CopyTo(destStore);
						int block_size = entry.Count;
						a.WriteInt8(entry.FirstEntry);
						a.WriteInt8(entry.LastEntry);
						a.WriteInt8(b_p);
						a.WriteInt4(block_size | (((int)entry.CompactType) << 24));
					}

					// Now finish the area initialization
					a.Finish();

					// Return pointer to the new area in dest_store.
					return blockP;
				} finally {
					destStore.UnlockForWrite();
				}
			}

			/// <summary>
			/// Recursively calls through the block hierarchy and deletes and 
			/// blocks that can be deleted.
			/// </summary>
			/// <returns></returns>
			private bool DeleteBlockChain() {
				bool parentDeleted = true;
				if (parentBlock != null) {
					parentDeleted = parentBlock.DeleteBlockChain();
					if (parentDeleted) {
						parentBlock = null;
					}
				}

				// If the parent is deleted,
				if (parentDeleted) {
					// Can we delete this block?
					if (referenceCount <= 0) {
						if (deleted && deletedAreas != null) {
							store.DeleteAllAreas(deletedAreas);
						}
						deletedAreas = null;
					} else {
						// We can't delete this block so return false
						return false;
					}
				}

				return parentDeleted;
			}

			/// <summary>
			/// Adds a reference to this object.
			/// </summary>
			public void AddReference() {
				lock (this) {
					if (freed)
						throw new Exception("Assertion failed: Block was freed.");

					++referenceCount;
				}
			}

			/// <summary>
			/// Removes a reference to this object.
			/// </summary>
			public void RemoveReference() {
				bool pendingDelete = false;
				lock (this) {
					--referenceCount;
					if (referenceCount <= 0) {
						if (freed)
							throw new Exception("Assertion failed: remove reference called too many times.");

						if (!deleted && deletedAreas != null)
							throw new Exception("Assertion failed: !deleted and deleted_areas != null");

						freed = true;
						if (deleted) {
							AddDeletedArea(indexBlockPointer);
							// Delete these areas
							pendingDelete = true;
						}
					}
				} // lock(this)

				if (pendingDelete) {
					lock (store) {
						DeleteBlockChain();
					}
				}
			}

			/// <summary>
			/// Returns the block size that has been set on this list.
			/// </summary>
			public int BlockSize {
				get { return blockSize; }
			}

			/// <summary>
			/// Returns the pointer to this index block in the store.
			/// </summary>
			public long Pointer {
				get { return indexBlockPointer; }
			}

			/// <summary>
			/// Marks this block as deleted.
			/// </summary>
			public void MarkAsDeleted() {
				lock (this) {
					deleted = true;
				}
			}

			/// <summary>
			/// Adds to the list of deleted areas in this block.
			/// </summary>
			/// <param name="pointer"></param>
			public void AddDeletedArea(long pointer) {
				lock (this) {
					if (deletedAreas == null) {
						deletedAreas = new List<long>();
					}

					deletedAreas.Add(pointer);
				}
			}
		} 
	}
}