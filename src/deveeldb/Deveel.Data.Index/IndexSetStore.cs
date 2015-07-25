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
using System.IO;
using System.Linq;
using System.Xml.Schema;

using Deveel.Data.DbSystem;
using Deveel.Data.Store;

namespace Deveel.Data.Index {
	class IndexSetStore {
		private IArea startArea;

		private long indexHeaderPointer;
		private IArea indexHeaderArea;

		private IndexBlock[] indexBlocks;

		private bool disposed;

		private const int Magic = 0x0CA90291;

		public IndexSetStore(IDatabaseContext databaseContext, IStore store) {
			Store = store;
		}

		public IStore Store { get; private set; }

		internal void DeleteAreas(IEnumerable<int> deletedAreas) {
			lock (this) {
				if (Store != null) {
					try {
						Store.LockForWrite();

						foreach (var id in deletedAreas) {
							Store.DeleteArea(id);
						}

					} catch (IOException e) {
						// TODO: Raise the error to the system
					} finally {
						Store.UnlockForWrite();
					}
				}
			}
		}

		private long CreateNewBlock() {
			// Allocate the area
			var a = Store.CreateArea(16);
			long indexBlockP = a.Id;
			// Setup the header
			a.WriteInt4(1);     // version
			a.WriteInt4(0);     // reserved
			a.WriteInt8(0);    // block entries
			a.Flush();

			return indexBlockP;
		}

		public long Create() {
			lock (this) {
				// Create an empty index header area
				var a = Store.CreateArea(16);
				indexHeaderPointer = a.Id;
				a.WriteInt4(1); // version
				a.WriteInt4(0); // reserved
				a.WriteInt8(0); // number of indexes in the set
				a.Flush();

				// Set up the local IArea object for the index header
				indexHeaderArea = Store.GetArea(indexHeaderPointer);

				indexBlocks = new IndexBlock[0];

				// Allocate the starting header
				var sa = Store.CreateArea(32);
				long startPointer = sa.Id;
				// The magic
				sa.WriteInt4(Magic);
				// The version
				sa.WriteInt4(1);
				// Pointer to the index header
				sa.WriteInt8(indexHeaderPointer);
				sa.Flush();

				// Set the 'start_area' value.
				startArea = Store.GetArea(startPointer);

				return startPointer;
			}
		}

		public void Open(long startPointer) {
			lock (this) {
				// Set up the start area
				startArea = Store.GetArea(startPointer);

				int magic = startArea.ReadInt4();
				if (magic != Magic)
					throw new IOException("Magic value for index set does not match.");

				int version = startArea.ReadInt4();
				if (version != 1)
					throw new IOException("Unknown version for index set.");

				// Setup the index_header area
				indexHeaderPointer = startArea.ReadInt8();
				indexHeaderArea = Store.GetArea(indexHeaderPointer);

				// Read the index header area
				version = indexHeaderArea.ReadInt4(); // version
				if (version != 1)
					throw new IOException("Incorrect version");

				indexHeaderArea.ReadInt4(); // reserved
				int indexCount = (int)indexHeaderArea.ReadInt8();
				indexBlocks = new IndexBlock[indexCount];

				// Initialize each index block
				for (int i = 0; i < indexCount; ++i) {
					int type = indexHeaderArea.ReadInt4();
					int blockSize = indexHeaderArea.ReadInt4();
					long indexBlockPointer = indexHeaderArea.ReadInt8();
					if (type != 1)
						throw new IOException("Do not understand index type: " + type);

					indexBlocks[i] = new IndexBlock(this, i, blockSize, indexBlockPointer);
					indexBlocks[i].AddReference();
				}
			}
		}

		public void Close() {
			lock (this) {
				if (Store != null) {
					for (int i = 0; i < indexBlocks.Length; ++i) {
						indexBlocks[i].RemoveReference();
					}
					Store = null;
					indexBlocks = null;
				}
			}
		}

		public void GetAreasUsed(List<long> list) {
			list.Add(startArea.Id);
			list.Add(indexHeaderPointer);

			foreach (var block in indexBlocks) {
				list.Add(block.StartOffset);
				long[] blockPointers = block.GetBlockPointers();
				list.AddRange(blockPointers);
			}
		}

		public void PrepareIndexLists(int count, int type, int blockSize) {
			lock (this) {
				try {
					Store.LockForWrite();

					// Allocate a new area for the list
					int newSize = 16 + ((indexBlocks.Length + count) * 16);
					var newIndexArea = Store.CreateArea(newSize);
					long newIndexPointer = newIndexArea.Id;
					var newIndexBlocks = new IndexBlock[(indexBlocks.Length + count)];

					// Copy the existing area
					indexHeaderArea.Position = 0;
					int version = indexHeaderArea.ReadInt4();
					int reserved = indexHeaderArea.ReadInt4();
					long icount = indexHeaderArea.ReadInt8();
					newIndexArea.WriteInt4(version);
					newIndexArea.WriteInt4(reserved);
					newIndexArea.WriteInt8(icount + count);

					for (int i = 0; i < indexBlocks.Length; ++i) {
						int itype = indexHeaderArea.ReadInt4();
						int iblockSize = indexHeaderArea.ReadInt4();
						long indexBlockP = indexHeaderArea.ReadInt8();

						newIndexArea.WriteInt4(itype);
						newIndexArea.WriteInt4(iblockSize);
						newIndexArea.WriteInt8(indexBlockP);

						newIndexBlocks[i] = indexBlocks[i];
					}

					// Add the new entries
					for (int i = 0; i < count; ++i) {
						long newBlankBlockP = CreateNewBlock();

						newIndexArea.WriteInt4(type);
						newIndexArea.WriteInt4(blockSize);
						newIndexArea.WriteInt8(newBlankBlockP);

						var newBlock = new IndexBlock(this, indexBlocks.Length + i, blockSize, newBlankBlockP);
						newBlock.AddReference();
						newIndexBlocks[indexBlocks.Length + i] = newBlock;
					}

					// Finished initializing the index.
					newIndexArea.Flush();

					// The old index header pointer
					long oldIndexHeaderP = indexHeaderPointer;

					// Update the state of this object,
					indexHeaderPointer = newIndexPointer;
					indexHeaderArea = Store.GetArea(newIndexPointer);
					indexBlocks = newIndexBlocks;

					// Update the start pointer
					startArea.Position = 8;
					startArea.WriteInt8(newIndexPointer);
					startArea.Flush();

					// Free the old header
					Store.DeleteArea(oldIndexHeaderP);
				} finally {
					Store.UnlockForWrite();
				}
			}
		}

		public IIndexSet GetSnapshotIndex() {
			lock (this) {
				// Clone the blocks list.  This represents the current snapshot of the
				// index state.
				var snapshotIndexBlocks = (IndexBlock[])indexBlocks.Clone();

				// Add this as the reference
				for (int i = 0; i < snapshotIndexBlocks.Length; ++i) {
					snapshotIndexBlocks[i].AddReference();
				}

				return new SnapshotIndexSet(this, snapshotIndexBlocks);
			}
		}

		private void CommitIndexHeader() {
			lock (this) {
				// Make a new index header area for the changed set.
				var a = Store.CreateArea(16 + (indexBlocks.Length * 16));
				long aOffset = a.Id;

				a.WriteInt4(1); // version
				a.WriteInt4(0); // reserved
				a.WriteInt8(indexBlocks.Length); // count

				foreach (var indBlock in indexBlocks) {
					a.WriteInt4(1);
					a.WriteInt4(indBlock.BlockSize);
					a.WriteInt8(indBlock.StartOffset);
				}

				// Finish creating the updated header
				a.Flush();

				// The old index header pointer
				long oldIndexHeaderP = indexHeaderPointer;

				// Set the new index header
				indexHeaderPointer = aOffset;
				indexHeaderArea = Store.GetArea(indexHeaderPointer);

				// Write the change to 'startPointer'
				startArea.Position = 8;
				startArea.WriteInt8(indexHeaderPointer);
				startArea.Flush();

				// Free the old header index
				Store.DeleteArea(oldIndexHeaderP);
			}
		}

		public void CommitIndexSet(IIndexSet indexSet) {
			var removedBlocks = new List<IndexBlock>();

			lock (this) {
				var sIndexSet = (SnapshotIndexSet)indexSet;
				var indices = sIndexSet.AllIndices.ToList();

				try {
					try {
						Store.LockForWrite();

						// For each Index in the index set,
						foreach (var index in indices) {
							int indexNum = index.IndexNumber;

							// The IndexBlock we are changing
							var curIndexBlock = indexBlocks[indexNum];

							// Get all the blocks in the list
							var blocks = index.AllBlocks.ToList();

							// Make up a new block list for this index set.
							var area = Store.CreateArea(16 + (blocks.Count * 28));
							long blockP = area.Id;
							area.WriteInt4(1);               // version
							area.WriteInt4(0);               // reserved
							area.WriteInt8(blocks.Count);  // block count

							foreach (var block in blocks) {
								var mappedBlock = (IMappedBlock)block;

								long bottomInt = 0;
								long topInt = 0;
								int blockSize = mappedBlock.Count;
								if (blockSize > 0) {
									bottomInt = mappedBlock.Bottom;
									topInt = mappedBlock.Top;
								}

								long blockPointer = mappedBlock.BlockPointer;

								// Is the block new or was it changed?
								if (blockPointer == -1 || mappedBlock.HasChanged) {
									// If this isn't -1 then write this sector on the list of
									// sectors to delete during GC.
									if (blockPointer != -1)
										curIndexBlock.AddDeletedArea(blockPointer);

									// This is a new block or a block that's been changed
									// Write the block to the file system
									blockPointer = mappedBlock.Flush();
								}

								area.WriteInt8(bottomInt);
								area.WriteInt8(topInt);
								area.WriteInt8(blockPointer);
								area.WriteInt4(blockSize | (((int)mappedBlock.CompactType) << 24));
							}

							// Finish initializing the area
							area.Flush();

							// Add the deleted blocks
							var deletedBlocks = index.DeletedBlocks.ToArray();
							for (int i = 0; i < deletedBlocks.Length; ++i) {
								long delBlockP = deletedBlocks[i].BlockPointer;
								if (delBlockP != -1)
									curIndexBlock.AddDeletedArea(delBlockP);
							}

							// Mark the current block as deleted
							curIndexBlock.MarkAsDeleted();

							// Now create a new IndexBlock object
							var newIndexBlock = new IndexBlock(this, indexNum, curIndexBlock.BlockSize, blockP);
							newIndexBlock.Parent = curIndexBlock;

							// Add reference to the new one
							newIndexBlock.AddReference();

							// Update the index_blocks list
							indexBlocks[indexNum] = newIndexBlock;

							// We remove this later.
							removedBlocks.Add(curIndexBlock);
						}

						// Commit the new index header (index_blocks)
						CommitIndexHeader();
					} finally {
						Store.UnlockForWrite();
					}

					// Commit finished.

				} catch (IOException e) {
					throw new InvalidOperationException("Error while committing index changed", e);
				}

			}

			// Remove all the references for the changed blocks,
			foreach (var block in removedBlocks) {
				block.RemoveReference();
			}
		}

		public void CommitDropIndex(int indexNum) {
			lock (this) {
				// The IndexBlock we are dropping
				var curIndexBlock = indexBlocks[indexNum];
				int blockSize = curIndexBlock.BlockSize;

				try {
					Store.LockForWrite();

					// Add all the elements to the deleted areas in the block
					var allBlockPointers = curIndexBlock.GetBlockPointers();
					foreach (long area in allBlockPointers) {
						curIndexBlock.AddDeletedArea(area);
					}

					// Mark the current block as deleted
					curIndexBlock.MarkAsDeleted();

					// Make up a new blank block list for this index set.
					long blockP = CreateNewBlock();

					// Now create a new IndexBlock object
					var newIndexBlock = new IndexBlock(this, indexNum, blockSize, blockP);

					// Add reference to the new one
					newIndexBlock.AddReference();
					// Remove reference to the old
					curIndexBlock.RemoveReference();
					// Update the index_blocks list
					indexBlocks[indexNum] = newIndexBlock;

					// Commit the new index header (index_blocks)
					CommitIndexHeader();
				} finally {
					Store.UnlockForWrite();
				}
			}
		}

		#region IndexBlock

		#endregion
	}
}