// 
//  Copyright 2010-2015 Deveel
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
//

using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Store;
using Deveel.Data.Util;

namespace Deveel.Data.Index {
	internal class IndexBlock {
		private readonly IndexSetStore indexSetStore;
		private readonly int indexNum;
		private readonly long blockEntries;

		private List<int> deletedAreas; 

		private int refCount;

		public IndexBlock(IndexSetStore indexSetStore, int indexNum, int blockSize, long startOffset) {
			this.indexSetStore = indexSetStore;
			this.indexNum = indexNum;
			BlockSize = blockSize;
			StartOffset = startOffset;

			// Read the index count
			var indexBlockArea = indexSetStore.Store.GetArea(startOffset);
			indexBlockArea.Position = 8;
			blockEntries = indexBlockArea.ReadInt8();

			refCount = 0;
		}

		public IndexBlock Parent { get; set; }

		public bool IsFreed { get; private set; }

		public bool IsDeleted { get; private set; }

		public int BlockSize { get; private set; }

		public long StartOffset { get; private set; }

		public long[] GetBlockPointers() {
			// Create an area for the index block pointer
			var indexBlockArea = indexSetStore.Store.GetArea(StartOffset);

			// First create the list of block entries for this list      
			long[] blocks = new long[(int)blockEntries];
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

		private IEnumerable<IMappedBlock> CreateMappedBlocks() {
			// Create an area for the index block pointer
			var indexBlockArea = indexSetStore.Store.GetArea(StartOffset);

			// First create the list of block entries for this list      
			var blocks = new IMappedBlock[(int) blockEntries];
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
					byte type = (byte) (ByteBuffer.URShift(typeSize, 24) & 0x0F);

					blocks[i] = StoreIndex.NewMappedBlock(indexSetStore, (int)firstEntry, (int)lastEntry, blockPointer, elementCount, type,
						BlockSize);
				}
			}

			return blocks;
		}

		public IIndex CreateIndex() {
			// Create the MappedListBlock objects for this view
			var blocks = CreateMappedBlocks().Cast<IIndexBlock<int>>();
			// And return the Index
			return new StoreIndex(indexSetStore, indexNum, BlockSize, blocks);
		}

		public long CopyTo(IStore destStore) {
			// Create the MappedListBlock object list for this view
			var blocks = CreateMappedBlocks().ToArray();
			try {
				destStore.LockForWrite();
				// Create the header area in the store for this block
				var a = destStore.CreateArea(16 + (blocks.Length * 28));
				long blockP = a.Id;

				a.WriteInt4(1);               // version
				a.WriteInt4(0);               // reserved
				a.WriteInt8(blocks.Length);  // block count
				for (int i = 0; i < blocks.Length; ++i) {
					IMappedBlock entry = blocks[i];
					long blockPointer = entry.CopyTo(destStore);
					int size = entry.Count;
					a.WriteInt8(entry.FirstEntry);
					a.WriteInt8(entry.LastEntry);
					a.WriteInt8(blockPointer);
					a.WriteInt4(size | (((int)entry.CompactType) << 24));
				}

				a.Flush();

				// Return pointer to the new area in dest_store.
				return blockP;
			} finally {
				destStore.UnlockForWrite();
			}
		}

		private bool DeleteBlockChain() {
			bool parentDeleted = true;
			if (Parent != null) {
				parentDeleted = Parent.DeleteBlockChain();
				if (parentDeleted) {
					Parent = null;
				}
			}

			// If the parent is deleted,
			if (parentDeleted) {
				// Can we delete this block?
				if (refCount <= 0) {
					if (IsDeleted && deletedAreas != null) {
						indexSetStore.DeleteAreas(deletedAreas);
					}
					deletedAreas = null;
				} else {
					// We can't delete this block so return false
					return false;
				}
			}

			return parentDeleted;
		}

		public void AddReference() {
			lock (this) {
				if (IsFreed)
					throw new Exception("Assertion failed: Block was freed.");

				++refCount;
			}
		}

		public void RemoveReference() {
			bool pendingDelete = false;
			lock (this) {
				--refCount;
				if (refCount <= 0) {
					if (IsFreed)
						throw new Exception("Assertion failed: remove reference called too many times.");

					if (!IsDeleted && deletedAreas != null)
						throw new Exception("Assertion failed: not deleted and with deleted areas");

					IsFreed = true;

					if (IsDeleted) {
						AddDeletedArea(StartOffset);
						// Delete these areas
						pendingDelete = true;
					}
				}
			} // lock(this)

			if (pendingDelete) {
				lock (indexSetStore.Store) {
					DeleteBlockChain();
				}
			}
		}

		public void MarkAsDeleted() {
			lock (this) {
				IsDeleted = true;
			}
		}

		public void AddDeletedArea(long pointer) {
			lock (this) {
				if (deletedAreas == null) {
					deletedAreas = new List<int>();
				}

				deletedAreas.Add((int)pointer);
			}
		}
	}
}