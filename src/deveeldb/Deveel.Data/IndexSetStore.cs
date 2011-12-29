// 
//  Copyright 2010  Deveel
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
using System.IO;

using Deveel.Data.Store;

namespace Deveel.Data {
	/// <summary>
	/// A class that manages the storage of a set of transactional index 
	/// lists in a way that is fast to modify.
	/// </summary>
	/// <remarks>
	///  This class has a number of objectives:
	/// <list type="bullet">
	/// <item>To prevent corruption as best as possible.</item>
	/// <item>To be able to modify lists of integers very fast and persistantly.</item>
	/// <item>To have size optimization features such as defragmentation.</item>
	/// <item>To provide very fast searches on sorted lists (caching features).</item>
	/// <item>To be able to map a list to an <see cref="IIndex"/> interface.</item>
	/// </list>
	/// <para>
	/// This object uses a <see cref="IStore"/> instance as its backing medium.
	/// </para>
	/// <para>
	/// This store manages three types of areas; <i>Index header</i>, <i>Index block</i> and
	/// <i>Index element</i>.
	/// </para>
	/// <para>
	/// <b>Index header</b>: This area type contains an entry for each index being stored.
	/// The Index header contains a pointer to an <i>Index block</i> area for each index.
	/// The pointer to the <i>Index block</i> in this area changes whenever an index
	/// changes, or when new indexes are added or deleted from the store.
	/// </para>
	/// <para>
	/// <b>Index block</b>: This area contains a number of pointers to Index element blocks.
	/// The number of entries depends on the number of indices in the list.  Each
	/// entry contains the size of the block, the first and last entry of the block,
	/// and a pointer to the element block itself.  If an element of the index
	/// changes or elements are removed or deleted, this block does NOT change.
	/// This should be considered an immutable area.
	/// </para>
	/// <para>
	/// <b>Index element</b>: This area simply contains the actual values in a block of the
	/// index.  An Index element area does not change and should be considered an
	/// immutable area.
	/// </para>
	/// </remarks>
	sealed partial class IndexSetStore {
		/// <summary>
		/// The magic value that we use to mark the start area.
		/// </summary>
		private const int MAGIC = 0x0CA90291;

		/// <summary>
		/// The TransactionSystem for this index set.
		/// </summary>
		private readonly TransactionSystem system;

		/// <summary>
		/// The Store that contains all the data of the index store.
		/// </summary>
		private IStore store;

		/// <summary>
		/// The starting header of this index set.
		/// </summary>
		/// <remarks>
		/// This is a very small area that simply contains a magic value and a pointer to 
		/// the index header. This is the only <see cref="IMutableArea"/> object that is 
		/// required by the index set.
		/// </remarks>
		private IMutableArea startArea;

		/**
		 * The index header area.  The index header area contains an entry for each
		 * index being stored.  Each entry is 16 bytes in size and has a 16 byte
		 * header.
		 * <p>
		 * HEADER: ( version (int), reserved (int), index count (long) ) <br>
		 * ENTRY: ( type (int), block_size (int), index block pointer (long) )
		 */
		private long indexHeaderPointer;
		private IArea indexHeaderArea;

		/// <summary>
		/// The index blocks - one for each index being stored.
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
		private IndexBlock[] indexBlocks;

		/// <summary>
		/// Constructs the <see cref="IndexSetStore"/> over the given Store object.
		/// </summary>
		/// <param name="store"></param>
		/// <param name="system"></param>
		public IndexSetStore(IStore store, TransactionSystem system) {
			this.store = store;
			this.system = system;
		}

		/// <summary>
		/// Delete all areas specified in the list (as a list of <see cref="long"/>).
		/// </summary>
		/// <param name="list"></param>
		private void DeleteAllAreas(IList<long> list) {
			lock (this) {
				if (store != null) {

					try {
						store.LockForWrite();

						foreach (long id in list) {
							store.DeleteArea(id);
						}

					} catch (IOException e) {
						system.Logger.Error(this, "Error when freeing old index block.");
						system.Logger.Error(this, e);
					} finally {
						store.UnlockForWrite();
					}

				}
			}
		}


		// ---------- Private methods ----------

		/// <summary>
		/// Creates a new blank index block in the store and returns a pointer to the area.
		/// </summary>
		/// <returns></returns>
		private long CreateBlankIndexBlock() {
			// Allocate the area
			IAreaWriter a = store.CreateArea(16);
			long index_block_p = a.Id;
			// Setup the header
			a.WriteInt4(1);     // version
			a.WriteInt4(0);     // reserved
			a.WriteInt8(0);    // block entries
			a.Finish();

			return index_block_p;
		}

		// ---------- Public methods ----------

		/// <summary>
		/// Creates a new black index set store and returns a pointer to a static area 
		/// that is later used to reference this index set in this store.
		/// </summary>
		/// <remarks>
		/// Remember to synch after this is called.
		/// </remarks>
		/// <returns></returns>
		public long Create() {
			lock (this) {
				// Create an empty index header area
				IAreaWriter a = store.CreateArea(16);
				indexHeaderPointer = a.Id;
				a.WriteInt4(1); // version
				a.WriteInt4(0); // reserved
				a.WriteInt8(0); // number of indexes in the set
				a.Finish();

				// Set up the local IArea object for the index header
				indexHeaderArea = store.GetArea(indexHeaderPointer);

				indexBlocks = new IndexBlock[0];

				// Allocate the starting header
				IAreaWriter sa = store.CreateArea(32);
				long startPointer = sa.Id;
				// The magic
				sa.WriteInt4(MAGIC);
				// The version
				sa.WriteInt4(1);
				// Pointer to the index header
				sa.WriteInt8(indexHeaderPointer);
				sa.Finish();

				// Set the 'start_area' value.
				startArea = store.GetMutableArea(startPointer);

				return startPointer;
			}
		}

		/// <summary>
		/// Initializes this index set.
		/// </summary>
		/// <param name="startPointer"></param>
		/// <remarks>
		/// This must be called during general initialization of the table object.
		/// </remarks>
		public void Init(long startPointer) {
			lock (this) {
				// Set up the start area
				startArea = store.GetMutableArea(startPointer);

				int magic = startArea.ReadInt4();
				if (magic != MAGIC)
					throw new IOException("Magic value for index set does not match.");

				int version = startArea.ReadInt4();
				if (version != 1)
					throw new IOException("Unknown version for index set.");

				// Setup the index_header area
				indexHeaderPointer = startArea.ReadInt8();
				indexHeaderArea = store.GetArea(indexHeaderPointer);

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

		/// <summary>
		/// Closes this index set (cleans up).
		/// </summary>
		public void Close() {
			lock (this) {
				if (store != null) {
					for (int i = 0; i < indexBlocks.Length; ++i) {
						indexBlocks[i].RemoveReference();
					}
					store = null;
					indexBlocks = null;
				}
			}
		}

		/// <summary>
		/// Overwrites all existing index information in this store and sets it to a
		/// copy of the given <see cref="IIndexSet"/> object.
		/// </summary>
		/// <param name="indexSet">A snapshot as returned by the <see cref="GetSnapshotIndexSet"/>
		/// method but not necessarily generated from this index set.</param>
		/// <remarks>
		/// This will create a new structure within this store that contains the copied
		/// index data.  This overwrites any existing data in this store so care should
		/// be used when using this method.
		/// <para>
		/// This method is an optimized method of copying all the index data in an
		/// index set and only requires a small buffer in memory.  The index data
		/// in <paramref name="indexSet"/> is not altered in any way by using this.
		/// </para>
		/// </remarks>
		public void CopyAllFrom(IIndexSet indexSet) {
			lock (this) {
				// Assert that IndexSetStore is initialized
				if (indexBlocks == null)
					throw new Exception("Can't copy because this IndexSetStore is not initialized.");

				// Drop any indexes in this index store.
				for (int i = 0; i < indexBlocks.Length; ++i) {
					CommitDropIndex(i);
				}

				if (!(indexSet is SnapshotIndexSet))
					throw new Exception("Can not copy non-IndexSetStore IIndexSet");

				// Cast to SnapshotIndexSet
				SnapshotIndexSet snapshotIndexSet = (SnapshotIndexSet) indexSet;

				// The number of IndexBlock items to copy.
				int indexCount = snapshotIndexSet.IndexBlocks.Length;

				// Record the old indexHeaderPointer
				long oldIndexHeaderP = indexHeaderPointer;

				// Create the header in this store
				IAreaWriter a = store.CreateArea(16 + (16*indexCount));
				indexHeaderPointer = a.Id;
				a.WriteInt4(1); // version
				a.WriteInt4(0); // reserved
				a.WriteInt8(indexCount); // number of indexes in the set

				// Fill in the information from the index_set
				for (int i = 0; i < indexCount; ++i) {
					IndexBlock sourceBlock = snapshotIndexSet.IndexBlocks[i];

					long indexBlockPointer = sourceBlock.CopyTo(store);

					a.WriteInt4(1); // NOTE: Only support for block type 1
					a.WriteInt4(sourceBlock.BlockSize);
					a.WriteInt8(indexBlockPointer);
				}

				// The header area has now been initialized.
				a.Finish();

				// Modify the start area header to point to this new structure.
				startArea.Position = 8;
				startArea.WriteInt8(indexHeaderPointer);
				// Check out the change
				startArea.CheckOut();

				// Free space associated with the old header_p
				store.DeleteArea(oldIndexHeaderP);

				// Re-initialize the index
				Init(startArea.Id);
			}
		}

		/// <summary>
		/// Adds to the given <see cref="IList{T}"/> all the areas in the store 
		/// that are used by this structure.
		/// </summary>
		/// <param name="list"></param>
		public void AddAllAreasUsed(IList<long> list) {
			list.Add(startArea.Id);
			list.Add(indexHeaderPointer);
			for (int i = 0; i < indexBlocks.Length; ++i) {
				IndexBlock block = indexBlocks[i];
				list.Add(block.Pointer);
				long[] blockPointers = block.GetBlockPointers();
				for (int n = 0; n < blockPointers.Length; ++n) {
					list.Add(blockPointers[n]);
				}
			}
		}

		/// <summary>
		/// Adds a number of blank index tables to the index store.
		/// </summary>
		/// <param name="count"></param>
		/// <param name="type"></param>
		/// <param name="blockSize"></param>
		/// <remarks>
		/// For example, we may want this store to contain 16 index lists.
		/// <para>
		/// This doesn't write the updated information to the file. You must call 
		/// 'flush' to write the information to the store.
		/// </para>
		/// </remarks>
		public void AddIndexLists(int count, int type, int blockSize) {
			lock (this) {
				try {
					store.LockForWrite();

					// Allocate a new area for the list
					int newSize = 16 + ((indexBlocks.Length + count) * 16);
					IAreaWriter newIndexArea = store.CreateArea(newSize);
					long newIndexPointer = newIndexArea.Id;
					IndexBlock[] newIndexBlocks = new IndexBlock[(indexBlocks.Length + count)];

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
						long newBlankBlockP = CreateBlankIndexBlock();

						newIndexArea.WriteInt4(type);
						newIndexArea.WriteInt4(blockSize);
						newIndexArea.WriteInt8(newBlankBlockP);

						IndexBlock iBlock = new IndexBlock(this, indexBlocks.Length + i, blockSize, newBlankBlockP);
						iBlock.AddReference();
						newIndexBlocks[indexBlocks.Length + i] = iBlock;
					}

					// Finished initializing the index.
					newIndexArea.Finish();

					// The old index header pointer
					long oldIndexHeaderP = indexHeaderPointer;

					// Update the state of this object,
					indexHeaderPointer = newIndexPointer;
					indexHeaderArea = store.GetArea(newIndexPointer);
					indexBlocks = newIndexBlocks;

					// Update the start pointer
					startArea.Position = 8;
					startArea.WriteInt8(newIndexPointer);
					startArea.CheckOut();

					// Free the old header
					store.DeleteArea(oldIndexHeaderP);
				} finally {
					store.UnlockForWrite();
				}
			}
		}

		/// <summary>
		/// Returns a current snapshot of the current indexes that are committed 
		/// in this store.
		/// </summary>
		/// <remarks>
		/// The returned object can be used to create mutable <see cref="IIndex"/> objects. 
		/// The created index lists are isolated from changes made to the rest of the indexes after 
		/// this method returns.
		/// <para>
		/// A transaction must grab an IIndexSet object when it opens.
		/// </para>
		/// <para>
		/// We <b>must</b> guarentee that the <see cref="IIndexSet"/> is disposed when the 
		/// transaction finishes.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public IIndexSet GetSnapshotIndexSet() {
			lock (this) {
				// Clone the blocks list.  This represents the current snapshot of the
				// index state.
				IndexBlock[] snapshotIndexBlocks = (IndexBlock[])indexBlocks.Clone();

				// Add this as the reference
				for (int i = 0; i < snapshotIndexBlocks.Length; ++i) {
					snapshotIndexBlocks[i].AddReference();
				}

				return new SnapshotIndexSet(this, snapshotIndexBlocks);
			}
		}

		/// <summary>
		/// Commits the index header with the current values set in 'index_blocks'.
		/// </summary>
		private void CommitIndexHeader() {
			lock (this) {
				// Make a new index header area for the changed set.
				IAreaWriter a = store.CreateArea(16 + (indexBlocks.Length * 16));
				long a_p = a.Id;

				a.WriteInt4(1); // version
				a.WriteInt4(0); // reserved
				a.WriteInt8(indexBlocks.Length); // count

				for (int i = 0; i < indexBlocks.Length; ++i) {
					IndexBlock indBlock = indexBlocks[i];
					a.WriteInt4(1);
					a.WriteInt4(indBlock.BlockSize);
					a.WriteInt8(indBlock.Pointer);
				}

				// Finish creating the updated header
				a.Finish();

				// The old index header pointer
				long oldIndexHeaderP = indexHeaderPointer;

				// Set the new index header
				indexHeaderPointer = a_p;
				indexHeaderArea = store.GetArea(indexHeaderPointer);

				// Write the change to 'startPointer'
				startArea.Position = 8;
				startArea.WriteInt8(indexHeaderPointer);
				startArea.CheckOut();

				// Free the old header index
				store.DeleteArea(oldIndexHeaderP);

			}
		}

		/// <summary>
		/// Commits changes made to a snapshop of an <see cref="IIndexSet"/> as being 
		/// permanent changes to the state of the index store.
		/// </summary>
		/// <param name="indexSet"></param>
		/// <remarks>
		/// This will generate an error if the given IIndexSet is not the last set 
		/// returned from the <see cref="GetSnapshotIndexSet"/> method.
		/// <para>
		/// For this to be used, during the transaction commit function a <see cref="GetSnapshotIndexSet"/>
		/// must be obtained, changes made to it from info in the journal, then a call to this method.  
		/// There must be a guarentee that <see cref="GetSnapshotIndexSet"/> is not called again during 
		/// this process.
		/// </para>
		/// <para>
		/// We must be guarenteed that when this method is called no other calls to other methods 
		/// in this object can be called.
		/// </para>
		/// </remarks>
		public void CommitIndexSet(IIndexSet indexSet) {
			List<IndexBlock> removedBlocks = new List<IndexBlock>();

			lock (this) {
				SnapshotIndexSet sIndexSet = (SnapshotIndexSet)indexSet;
				IEnumerable<IIndex> indices = sIndexSet.AllIndices;

				try {
					try {
						store.LockForWrite();

						// For each Index in the index set,
						foreach (Index index in indices) {
							int indexNum = index.IndexNumber;

							// The IndexBlock we are changing
							IndexBlock curIndexBlock = indexBlocks[indexNum];

							// Get all the blocks in the list
							IList<IBlockIndexBlock> blocks = index.AllBlocks;

							// Make up a new block list for this index set.
							IAreaWriter a = store.CreateArea(16 + (blocks.Count * 28));
							long blockP = a.Id;
							a.WriteInt4(1);               // version
							a.WriteInt4(0);               // reserved
							a.WriteInt8(blocks.Count);  // block count
							for (int i = 0; i < blocks.Count; ++i) {
								IMappedBlock b = (IMappedBlock) blocks[i];

								long bottomInt = 0;
								long topInt = 0;
								int blockSize = b.Count;
								if (blockSize > 0) {
									bottomInt = b.Bottom;
									topInt = b.Top;
								}
								long b_p = b.BlockPointer;
								// Is the block new or was it changed?
								if (b_p == -1 || b.HasChanged) {
									// If this isn't -1 then WriteByte this sector on the list of
									// sectors to delete during GC.
									if (b_p != -1) {
										curIndexBlock.AddDeletedArea(b_p);
									}
									// This is a new block or a block that's been changed
									// Write the block to the file system
									b_p = b.WriteToStore();
								}
								a.WriteInt8(bottomInt);
								a.WriteInt8(topInt);
								a.WriteInt8(b_p);
								a.WriteInt4(blockSize | (((int)b.CompactType) << 24));

							}

							// Finish initializing the area
							a.Finish();

							// Add the deleted blocks
							IMappedBlock[] deletedBlocks = index.DeletedBlocks;
							for (int i = 0; i < deletedBlocks.Length; ++i) {
								long delBlockP = deletedBlocks[i].BlockPointer;
								if (delBlockP != -1)
									curIndexBlock.AddDeletedArea(delBlockP);
							}

							// Mark the current block as deleted
							curIndexBlock.MarkAsDeleted();

							// Now create a new IndexBlock object
							IndexBlock newIndexBlock = new IndexBlock(this, indexNum, curIndexBlock.BlockSize, blockP);
							newIndexBlock.SetParentIndexBlock(curIndexBlock);

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
						store.UnlockForWrite();
					}

					// Commit finished.

				} catch (IOException e) {
					system.Logger.Error(this, e);
					throw new ApplicationException("IO Error: " + e.Message, e);
				}

			} // lock

			// Remove all the references for the changed blocks,
			foreach (IndexBlock block in removedBlocks) {
				block.RemoveReference();
			}
		}

		/// <summary>
		/// Commits a change that drops an index from the index set.
		/// </summary>
		/// <param name="indexNum"></param>
		/// <remarks>
		/// This must be called from within the conglomerate commit. The actual implementation of this 
		/// overwrites the index with with a 0 length index. This is also useful if you want to reindex 
		/// a column.
		/// </remarks>
		public void CommitDropIndex(int indexNum) {
			lock (this) {
				// The IndexBlock we are dropping
				IndexBlock curIndexBlock = indexBlocks[indexNum];
				int blockSize = curIndexBlock.BlockSize;

				try {
					store.LockForWrite();

					// Add all the elements to the deleted areas in the block
					long[] allBlockPointers = curIndexBlock.GetBlockPointers();
					for (int i = 0; i < allBlockPointers.Length; ++i) {
						curIndexBlock.AddDeletedArea(allBlockPointers[i]);
					}

					// Mark the current block as deleted
					curIndexBlock.MarkAsDeleted();

					// Make up a new blank block list for this index set.
					long blockP = CreateBlankIndexBlock();

					// Now create a new IndexBlock object
					IndexBlock newIndexBlock = new IndexBlock(this, indexNum, blockSize, blockP);

					// Add reference to the new one
					newIndexBlock.AddReference();
					// Remove reference to the old
					curIndexBlock.RemoveReference();
					// Update the index_blocks list
					indexBlocks[indexNum] = newIndexBlock;

					// Commit the new index header (index_blocks)
					CommitIndexHeader();
				} finally {
					store.UnlockForWrite();
				}

			}
		}
	}
}