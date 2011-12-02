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
using System.Collections;
using System.IO;

using Deveel.Data.Collections;
using Deveel.Data.Store;

using Deveel.Diagnostics;
using Deveel.Data.Util;

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
	/// <item>To be able to map a list to an <see cref="IIntegerList"/> interface.</item>
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
	sealed class IndexSetStore {
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
		private IMutableArea start_area;

		/**
		 * The index header area.  The index header area contains an entry for each
		 * index being stored.  Each entry is 16 bytes in size and has a 16 byte
		 * header.
		 * <p>
		 * HEADER: ( version (int), reserved (int), index count (long) ) <br>
		 * ENTRY: ( type (int), block_size (int), index block pointer (long) )
		 */
		private long index_header_p;
		private IArea index_header_area;

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
		private IndexBlock[] index_blocks;

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
		private void DeleteAllAreas(IList list) {
			lock (this) {
				if (store != null) {

					try {
						store.LockForWrite();

						int sz = list.Count;
						for (int i = 0; i < sz; ++i) {
							long id = (long)list[i];
							store.DeleteArea(id);
						}

					} catch (IOException e) {
						system.Debug.Write(DebugLevel.Error, this, "Error when freeing old index block.");
						system.Debug.WriteException(e);
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
				index_header_p = a.Id;
				a.WriteInt4(1); // version
				a.WriteInt4(0); // reserved
				a.WriteInt8(0); // number of indexes in the set
				a.Finish();

				// Set up the local IArea object for the index header
				index_header_area = store.GetArea(index_header_p);

				index_blocks = new IndexBlock[0];

				// Allocate the starting header
				IAreaWriter start_a = store.CreateArea(32);
				long start_p = start_a.Id;
				// The magic
				start_a.WriteInt4(MAGIC);
				// The version
				start_a.WriteInt4(1);
				// Pointer to the index header
				start_a.WriteInt8(index_header_p);
				start_a.Finish();

				// Set the 'start_area' value.
				start_area = store.GetMutableArea(start_p);

				return start_p;
			}
		}

		/// <summary>
		/// Initializes this index set.
		/// </summary>
		/// <param name="start_p"></param>
		/// <remarks>
		/// This must be called during general initialization of the table object.
		/// </remarks>
		public void Init(long start_p) {
			lock (this) {
				// Set up the start area
				start_area = store.GetMutableArea(start_p);

				int magic = start_area.ReadInt4();
				if (magic != MAGIC) {
					throw new IOException("Magic value for index set does not match.");
				}
				int version = start_area.ReadInt4();
				if (version != 1) {
					throw new IOException("Unknown version for index set.");
				}

				// Setup the index_header area
				index_header_p = start_area.ReadInt8();
				index_header_area = store.GetArea(index_header_p);

				// Read the index header area
				version = index_header_area.ReadInt4(); // version
				if (version != 1) {
					throw new IOException("Incorrect version");
				}
				int reserved = index_header_area.ReadInt4(); // reserved
				int index_count = (int)index_header_area.ReadInt8();
				index_blocks = new IndexBlock[index_count];

				// Initialize each index block
				for (int i = 0; i < index_count; ++i) {
					int type = index_header_area.ReadInt4();
					int block_size = index_header_area.ReadInt4();
					long index_block_p = index_header_area.ReadInt8();
					if (type == 1) {
						index_blocks[i] = new IndexBlock(this, i, block_size, index_block_p);
						index_blocks[i].AddReference();
					} else {
						throw new IOException("Do not understand index type: " + type);
					}
				}

			}
		}

		/// <summary>
		/// Closes this index set (cleans up).
		/// </summary>
		public void Close() {
			lock (this) {
				if (store != null) {
					for (int i = 0; i < index_blocks.Length; ++i) {
						index_blocks[i].RemoveReference();
					}
					store = null;
					index_blocks = null;
				}
			}
		}

		/// <summary>
		/// Overwrites all existing index information in this store and sets it to a
		/// copy of the given <see cref="IIndexSet"/> object.
		/// </summary>
		/// <param name="index_set">A snapshot as returned by the <see cref="GetSnapshotIndexSet"/>
		/// method but not necessarily generated from this index set.</param>
		/// <remarks>
		/// This will create a new structure within this store that contains the copied
		/// index data.  This overwrites any existing data in this store so care should
		/// be used when using this method.
		/// <para>
		/// This method is an optimized method of copying all the index data in an
		/// index set and only requires a small buffer in memory.  The index data
		/// in <paramref name="index_set"/> is not altered in any way by using this.
		/// </para>
		/// </remarks>
		public void CopyAllFrom(IIndexSet index_set) {
			lock (this) {
				// Assert that IndexSetStore is initialized
				if (index_blocks == null) {
					throw new Exception(
						"Can't copy because this IndexSetStore is not initialized.");
				}

				// Drop any indexes in this index store.
				for (int i = 0; i < index_blocks.Length; ++i) {
					CommitDropIndex(i);
				}

				if (index_set is SnapshotIndexSet) {
					// Cast to SnapshotIndexSet
					SnapshotIndexSet s_index_set = (SnapshotIndexSet)index_set;

					// The number of IndexBlock items to copy.
					int index_count = s_index_set.snapshot_index_blocks.Length;

					// Record the old index_header_p
					long old_index_header_p = index_header_p;

					// Create the header in this store
					IAreaWriter a = store.CreateArea(16 + (16 * index_count));
					index_header_p = a.Id;
					a.WriteInt4(1); // version
					a.WriteInt4(0); // reserved
					a.WriteInt8(index_count); // number of indexes in the set

					// Fill in the information from the index_set
					for (int i = 0; i < index_count; ++i) {
						IndexBlock source_block = s_index_set.snapshot_index_blocks[i];

						long index_block_p = source_block.CopyTo(store);

						a.WriteInt4(1); // NOTE: Only support for block type 1
						a.WriteInt4(source_block.BlockSize);
						a.WriteInt8(index_block_p);
					}

					// The header area has now been initialized.
					a.Finish();

					// Modify the start area header to point to this new structure.
					start_area.Position = 8;
					start_area.WriteInt8(index_header_p);
					// Check out the change
					start_area.CheckOut();

					// Free space associated with the old header_p
					store.DeleteArea(old_index_header_p);
				} else {
					throw new Exception("Can not copy non-IndexSetStore IIndexSet");
				}

				// Re-initialize the index
				Init(start_area.Id);
			}
		}

		/// <summary>
		/// Adds to the given <see cref="ArrayList"/> all the areas in the store 
		/// that are used by this structure (as Long).
		/// </summary>
		/// <param name="list"></param>
		public void AddAllAreasUsed(ArrayList list) {
			list.Add(start_area.Id);
			list.Add(index_header_p);
			for (int i = 0; i < index_blocks.Length; ++i) {
				IndexBlock block = index_blocks[i];
				list.Add(block.Pointer);
				long[] block_pointers = block.AllBlockPointers;
				for (int n = 0; n < block_pointers.Length; ++n) {
					list.Add(block_pointers[n]);
				}
			}
		}

		/// <summary>
		/// Adds a number of blank index tables to the index store.
		/// </summary>
		/// <param name="count"></param>
		/// <param name="type"></param>
		/// <param name="block_size"></param>
		/// <remarks>
		/// For example, we may want this store to contain 16 index lists.
		/// <para>
		/// This doesn't write the updated information to the file. You must call 
		/// 'flush' to write the information to the store.
		/// </para>
		/// </remarks>
		public void AddIndexLists(int count, int type, int block_size) {
			lock (this) {
				try {
					store.LockForWrite();

					// Allocate a new area for the list
					int new_size = 16 + ((index_blocks.Length + count) * 16);
					IAreaWriter new_index_area = store.CreateArea(new_size);
					long new_index_p = new_index_area.Id;
					IndexBlock[] new_index_blocks =
						new IndexBlock[(index_blocks.Length + count)];

					// Copy the existing area
					index_header_area.Position = 0;
					int version = index_header_area.ReadInt4();
					int reserved = index_header_area.ReadInt4();
					long icount = index_header_area.ReadInt8();
					new_index_area.WriteInt4(version);
					new_index_area.WriteInt4(reserved);
					new_index_area.WriteInt8(icount + count);

					for (int i = 0; i < index_blocks.Length; ++i) {
						int itype = index_header_area.ReadInt4();
						int iblock_size = index_header_area.ReadInt4();
						long index_block_p = index_header_area.ReadInt8();

						new_index_area.WriteInt4(itype);
						new_index_area.WriteInt4(iblock_size);
						new_index_area.WriteInt8(index_block_p);

						new_index_blocks[i] = index_blocks[i];
					}

					// Add the new entries
					for (int i = 0; i < count; ++i) {
						long new_blank_block_p = CreateBlankIndexBlock();

						new_index_area.WriteInt4(type);
						new_index_area.WriteInt4(block_size);
						new_index_area.WriteInt8(new_blank_block_p);

						IndexBlock i_block = new IndexBlock(this, index_blocks.Length + i,
															block_size, new_blank_block_p);
						i_block.AddReference();
						new_index_blocks[index_blocks.Length + i] = i_block;

					}

					// Finished initializing the index.
					new_index_area.Finish();

					// The old index header pointer
					long old_index_header_p = index_header_p;

					// Update the state of this object,
					index_header_p = new_index_p;
					index_header_area = store.GetArea(new_index_p);
					index_blocks = new_index_blocks;

					// Update the start pointer
					start_area.Position = 8;
					start_area.WriteInt8(new_index_p);
					start_area.CheckOut();

					// Free the old header
					store.DeleteArea(old_index_header_p);

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
		/// The returned object can be used to create mutable <see cref="IIntegerList"/> objects. 
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
				IndexBlock[] snapshot_index_blocks = (IndexBlock[])index_blocks.Clone();

				// Add this as the reference
				for (int i = 0; i < snapshot_index_blocks.Length; ++i) {
					snapshot_index_blocks[i].AddReference();
				}

				return new SnapshotIndexSet(this, snapshot_index_blocks);
			}
		}

		/// <summary>
		/// Commits the index header with the current values set in 'index_blocks'.
		/// </summary>
		private void CommitIndexHeader() {
			lock (this) {
				// Make a new index header area for the changed set.
				IAreaWriter a = store.CreateArea(16 + (index_blocks.Length * 16));
				long a_p = a.Id;

				a.WriteInt4(1); // version
				a.WriteInt4(0); // reserved
				a.WriteInt8(index_blocks.Length); // count

				for (int i = 0; i < index_blocks.Length; ++i) {
					IndexBlock ind_block = index_blocks[i];
					a.WriteInt4(1);
					a.WriteInt4(ind_block.BlockSize);
					a.WriteInt8(ind_block.Pointer);
				}

				// Finish creating the updated header
				a.Finish();

				// The old index header pointer
				long old_index_header_p = index_header_p;

				// Set the new index header
				index_header_p = a_p;
				index_header_area = store.GetArea(index_header_p);

				// Write the change to 'start_p'
				start_area.Position = 8;
				start_area.WriteInt8(index_header_p);
				start_area.CheckOut();

				// Free the old header index
				store.DeleteArea(old_index_header_p);

			}
		}

		/// <summary>
		/// Commits changes made to a snapshop of an <see cref="IIndexSet"/> as being 
		/// permanent changes to the state of the index store.
		/// </summary>
		/// <param name="index_set"></param>
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
		public void CommitIndexSet(IIndexSet index_set) {

			ArrayList removed_blocks = new ArrayList();

			lock (this) {

				SnapshotIndexSet s_index_set = (SnapshotIndexSet)index_set;
				IndexIntegerList[] lists = s_index_set.AllLists;

				try {

					try {
						store.LockForWrite();

						// For each IndexIntegerList in the index set,
						for (int n = 0; n < lists.Length; ++n) {
							// Get the list
							IndexIntegerList list = (IndexIntegerList)lists[n];
							int index_num = list.IndexNumber;
							// The IndexBlock we are changing
							IndexBlock cur_index_block = index_blocks[index_num];
							// Get all the blocks in the list
							MappedListBlock[] blocks = list.AllBlocks;

							// Make up a new block list for this index set.
							IAreaWriter a = store.CreateArea(16 + (blocks.Length * 28));
							long block_p = a.Id;
							a.WriteInt4(1);               // version
							a.WriteInt4(0);               // reserved
							a.WriteInt8(blocks.Length);  // block count
							for (int i = 0; i < blocks.Length; ++i) {
								MappedListBlock b = blocks[i];

								long bottom_int = 0;
								long top_int = 0;
								int block_size = b.Count;
								if (block_size > 0) {
									bottom_int = b.Bottom;
									top_int = b.Top;
								}
								long b_p = b.BlockPointer;
								// Is the block new or was it changed?
								if (b_p == -1 || b.HasChanged) {
									// If this isn't -1 then WriteByte this sector on the list of
									// sectors to delete during GC.
									if (b_p != -1) {
										cur_index_block.AddDeletedArea(b_p);
									}
									// This is a new block or a block that's been changed
									// Write the block to the file system
									b_p = b.WriteToStore();
								}
								a.WriteInt8(bottom_int);
								a.WriteInt8(top_int);
								a.WriteInt8(b_p);
								a.WriteInt4(block_size | (((int)b.CompactType) << 24));

							}

							// Finish initializing the area
							a.Finish();

							// Add the deleted blocks
							MappedListBlock[] deleted_blocks = list.DeletedBlocks;
							for (int i = 0; i < deleted_blocks.Length; ++i) {
								long del_block_p = deleted_blocks[i].BlockPointer;
								if (del_block_p != -1) {
									cur_index_block.AddDeletedArea(del_block_p);
								}
							}

							// Mark the current block as deleted
							cur_index_block.MarkAsDeleted();

							// Now create a new IndexBlock object
							IndexBlock new_index_block =
							   new IndexBlock(this, index_num, cur_index_block.BlockSize, block_p);
							new_index_block.SetParentIndexBlock(cur_index_block);

							// Add reference to the new one
							new_index_block.AddReference();
							// Update the index_blocks list
							index_blocks[index_num] = new_index_block;

							// We remove this later.
							removed_blocks.Add(cur_index_block);

						}

						// Commit the new index header (index_blocks)
						CommitIndexHeader();

					} finally {
						store.UnlockForWrite();
					}

					// Commit finished.

				} catch (IOException e) {
					system.Debug.WriteException(e);
					throw new ApplicationException("IO Error: " + e.Message);
				}

			} // synchronized

			// Remove all the references for the changed blocks,
			int sz = removed_blocks.Count;
			for (int i = 0; i < sz; ++i) {
				IndexBlock block = (IndexBlock)removed_blocks[i];
				block.RemoveReference();
			}

		}

		/// <summary>
		/// Commits a change that drops an index from the index set.
		/// </summary>
		/// <param name="index_num"></param>
		/// <remarks>
		/// This must be called from within the conglomerate commit. The actual implementation of this 
		/// overwrites the index with with a 0 length index. This is also useful if you want to reindex 
		/// a column.
		/// </remarks>
		public void CommitDropIndex(int index_num) {
			lock (this) {
				// The IndexBlock we are dropping
				IndexBlock cur_index_block = index_blocks[index_num];
				int block_size = cur_index_block.BlockSize;

				try {
					store.LockForWrite();

					// Add all the elements to the deleted areas in the block
					long[] all_block_pointers = cur_index_block.AllBlockPointers;
					for (int i = 0; i < all_block_pointers.Length; ++i) {
						cur_index_block.AddDeletedArea(all_block_pointers[i]);
					}

					// Mark the current block as deleted
					cur_index_block.MarkAsDeleted();

					// Make up a new blank block list for this index set.
					long block_p = CreateBlankIndexBlock();

					// Now create a new IndexBlock object
					IndexBlock new_index_block = new IndexBlock(this, index_num, block_size, block_p);

					// Add reference to the new one
					new_index_block.AddReference();
					// Remove reference to the old
					cur_index_block.RemoveReference();
					// Update the index_blocks list
					index_blocks[index_num] = new_index_block;

					// Commit the new index header (index_blocks)
					CommitIndexHeader();

				} finally {
					store.UnlockForWrite();
				}

			}
		}


		// ---------- Inner classes ----------


		/// <summary>
		/// A convenience static empty integer list array.
		/// </summary>
		private static readonly IndexIntegerList[] EmptyIntegerLists = new IndexIntegerList[0];


		/// <summary>
		/// The implementation of IIndexSet which represents a mutable snapshot of 
		/// the indices stored in this set.
		/// </summary>
		private class SnapshotIndexSet : IIndexSet {
			private readonly IndexSetStore set_store;

			/// <summary>
			/// The list of IndexBlock object that represent the view of the index set
			/// when the view was created.
			/// </summary>
			internal IndexBlock[] snapshot_index_blocks;

			/// <summary>
			/// The list of <see cref="IndexIntegerList"/> objects that have been returned 
			/// via the <see cref="GetIndex"/> method.
			/// </summary>
			private ArrayList integer_lists;

			/// <summary>
			/// Set to true when this object is disposed.
			/// </summary>
			private bool disposed;


			public SnapshotIndexSet(IndexSetStore set_store, IndexBlock[] blocks) {
				this.set_store = set_store;
				snapshot_index_blocks = blocks;

				// Not disposed.
				disposed = false;

			}

			~SnapshotIndexSet() {
				Dispose(false);
			}

			/// <summary>
			/// Returns all the lists that have been created by calls to <see cref="GetIndex"/>.
			/// </summary>
			public IndexIntegerList[] AllLists {
				get {
					if (integer_lists == null) {
						return EmptyIntegerLists;
					}
					else {
						return (IndexIntegerList[]) integer_lists.ToArray(typeof (IndexIntegerList));
					}
				}
			}

			// ---------- Implemented from IIndexSet ----------

			public IIntegerList GetIndex(int n) {
				// Create if not exist.
				if (integer_lists == null) {
					integer_lists = new ArrayList();
				} else {
					// If this list has already been created, return it
					for (int o = 0; o < integer_lists.Count; ++o) {
						IndexIntegerList i_list = (IndexIntegerList)integer_lists[o];
						if (i_list.IndexNumber == n) {
							return i_list;
							//            throw new Error(
							//                        "IIntegerList already created for this n.");
						}
					}
				}

				try {

					IndexIntegerList ilist =
									   snapshot_index_blocks[n].CreateIndexIntegerList();
					integer_lists.Add(ilist);
					return ilist;

				} catch (IOException e) {
					set_store.system.Debug.WriteException(e);
					throw new Exception("IO Error: " + e.Message);
				}

			}

			private void Dispose() {
				if (!disposed) {

					if (integer_lists != null) {
						for (int i = 0; i < integer_lists.Count; ++i) {
							IndexIntegerList ilist = (IndexIntegerList)integer_lists[i];
							ilist.Dispose();
						}
						integer_lists = null;
					}

					// Release reference to the index_blocks;
					for (int i = 0; i < snapshot_index_blocks.Length; ++i) {
						IndexBlock iblock = snapshot_index_blocks[i];
						iblock.RemoveReference();
					}
					snapshot_index_blocks = null;

					disposed = true;
				}
			}

			void IDisposable.Dispose() {
				GC.SuppressFinalize(this);
				Dispose(true);
			}

			private void Dispose(bool disposing) {
				if (disposing) {
					try {
						if (!disposed) {
							Dispose();
						}
					} catch (Exception e) {
						set_store.system.Debug.Write(DebugLevel.Error, this, "Finalize error: " + e.Message);
						set_store.system.Debug.WriteException(e);
					}					
				}
			}
		}

		/// <summary>
		/// An <see cref="IntegerListBlockInterface"/> implementation that maps a block 
		/// of a list to an underlying file system representation.
		/// </summary>
		internal sealed class MappedListBlock : BlockIntegerList.IntArrayListBlock {
			private readonly IndexSetStore set_store;

			/// <summary>
			/// The first entry in the block.
			/// </summary>
			internal readonly long first_entry;

			/// <summary>
			/// The last entry in the block.
			/// </summary>
			internal readonly long last_entry;

			/// <summary>
			/// A pointer to the area where this block can be found.
			/// </summary>
			private long block_p;

			/// <summary>
			/// Lock object.
			/// </summary>
			private Object m_lock = new Object();

			/// <summary>
			/// Set to true if the loaded block is mutable.
			/// </summary>
			private bool mutable_block;

			/// <summary>
			/// How this block is compacted in the store.  If this is 1 the elements 
			/// are stored as shorts, if it is 2 - ints, and if it is 3 - longs.
			/// </summary>
			private byte compact_type;

			/// <summary>
			/// The maximum size of the block.
			/// </summary>
			private readonly int max_block_size;

			public MappedListBlock(IndexSetStore set_store, long first_e, long last_e,
								   long mapped_p, int size, byte compact_type,
								   int max_block_size) {
				this.set_store = set_store;
				this.first_entry = first_e;
				this.last_entry = last_e;
				this.block_p = mapped_p;
				this.compact_type = compact_type;
				this.max_block_size = max_block_size;
				count = size;
				array = null;
			}

			/// <summary>
			/// Creates an empty block.
			/// </summary>
			/// <param name="set_store"></param>
			/// <param name="block_size_in"></param>
			public MappedListBlock(IndexSetStore set_store, int block_size_in)
				: base(block_size_in) {
				this.set_store = set_store;
				this.block_p = -1;
				this.max_block_size = block_size_in;
			}

			/// <summary>
			/// Returns a pointer to the area that contains this block.
			/// </summary>
			public long BlockPointer {
				get { return block_p; }
			}

			/// <summary>
			/// Returns the compact type of this block.
			/// </summary>
			public byte CompactType {
				get { return compact_type; }
			}

			/// <summary>
			/// Copies the index data in this block to a new block in the given store
			/// and returns a pointer to the new block.
			/// </summary>
			/// <param name="dest_store"></param>
			/// <returns></returns>
			public long CopyTo(IStore dest_store) {
				// The number of bytes per entry
				int entry_size = compact_type;
				// The total size of the entry.
				int area_size = (count * entry_size);

				// Allocate the destination area
				IAreaWriter dest = dest_store.CreateArea(area_size);
				long dest_block_p = dest.Id;
				set_store.store.GetArea(block_p).CopyTo(dest, area_size);
				dest.Finish();

				return dest_block_p;
			}

			/// <summary>
			/// Writes this block to a new sector in the index file and updates the
			/// information in this object accordingly.
			/// </summary>
			/// <returns>
			/// Returns the sector the block was written to.
			/// </returns>
			public long WriteToStore() {
				// Convert the int[] array to a byte[] array.

				// First determine how we compact this int array into a byte array.  If
				// all the values are < 32768 then we store as shorts
				long largest_val = 0;
				for (int i = 0; i < count; ++i) {
					long v = (long)array[i];
					if (System.Math.Abs(v) > System.Math.Abs(largest_val)) {
						largest_val = v;
					}
				}

				long lv = largest_val;
				if (lv >> 7 == 0 || lv >> 7 == -1) {
					compact_type = 1;
				} else if (lv >> 15 == 0 || lv >> 15 == -1) {
					compact_type = 2;
				} else if (lv >> 23 == 0 || lv >> 23 == -1) {
					compact_type = 3;
				}
					// NOTE: in the future we'll want to determine if we are going to store
					//   as an int or long array.
				  else {
					compact_type = 4;
				}

				// The number of bytes per entry
				int entry_size = compact_type;
				// The total size of the entry.
				int area_size = (count * entry_size);

				// Allocate an array to buffer the block to
				byte[] arr = new byte[area_size];
				// Fill the array
				int p = 0;
				for (int i = 0; i < count; ++i) {
					int v = array[i];
					for (int n = entry_size - 1; n >= 0; --n) {
						//TODO: check this...
						arr[p] = (byte)(ByteBuffer.URShift(v, (n * 8)) & 0x0FF);
						++p;
					}
				}

				// Create an area to store this
				IAreaWriter a = set_store.store.CreateArea(area_size);
				block_p = a.Id;
				// Write to the area
				a.Write(arr, 0, area_size);
				// And finish the area initialization
				a.Finish();

				// Once written, the block is invalidated
				m_lock = null;

				return block_p;
			}

			/// <summary>
			/// Overwritten from <see cref="BlockIntegerList.IntArrayListBlock"/>, this returns the 
			/// int[] array that contains the contents of the block.
			/// </summary>
			/// <param name="immutable"></param>
			/// <remarks>
			/// In this implementation, we determine if the array has been Read from the index file. If 
			/// it hasn't we read it in, otherwise we use the version in memory.
			/// </remarks>
			/// <returns></returns>
			protected override int[] GetArray(bool immutable) {
				// We must synchronize this entire block because otherwise we could
				// return a partially loaded array.
				lock (m_lock) {

					if (array != null) {
						PrepareMutate(immutable);
						return array;
					}

					// Create the int array
					array = new int[max_block_size];

					// The number of bytes per entry
					int entry_size = compact_type;
					// The total size of the entry.
					int area_size = (count * entry_size);

					// Read in the byte array
					byte[] buf = new byte[area_size];
					try {
						set_store.store.GetArea(block_p).Read(buf, 0, area_size);
					} catch (IOException e) {
						set_store.system.Debug.Write(DebugLevel.Error, this, "block_p = " + block_p);
						set_store.system.Debug.WriteException(e);
						throw new ApplicationException("IO Error: " + e.Message);
					}

					// Uncompact it into the int array
					int p = 0;
					for (int i = 0; i < count; ++i) {
						int v = (((int)buf[p]) << ((entry_size - 1) * 8));
						++p;
						for (int n = entry_size - 2; n >= 0; --n) {
							v = v | ((((int)buf[p]) & 0x0FF) << (n * 8));
							++p;
						}
						array[i] = v;
					}

					mutable_block = false;
					PrepareMutate(immutable);
					return array;

				}

			}

			/// <inheritdoc/>
			protected override int ArrayLength {
				get { return max_block_size; }
			}

			/// <summary>
			/// Makes the block mutable if it is immutable.
			/// </summary>
			/// <param name="immutable"></param>
			/// <remarks>
			/// We must be synchronized on <see cref="m_lock"/> before this 
			/// method is called.
			/// </remarks>
			private void PrepareMutate(bool immutable) {
				// If list is to be mutable
				if (!immutable && !mutable_block) {
					array = (int[])array.Clone();
					mutable_block = true;
				}
			}

			/// <inheritdoc/>
			public override int Top {
				get {
					if (count == 0) {
						throw new ApplicationException("No first int in block.");
					}

					lock (m_lock) {
						if (array == null) {
							return (int) last_entry;
						} else {
							return array[count - 1];
						}
					}
				}
			}

			/// <inheritdoc/>
			public override int Bottom {
				get {
					if (count == 0) {
						throw new ApplicationException("No first int in block.");
					}

					lock (m_lock) {
						if (array == null) {
							return (int) first_entry;
						} else {
							return array[0];
						}
					}
				}
			}
		}


		/// <summary>
		/// The <see cref="IIntegerList"/> implementation that is used to 
		/// represent a mutable snapshop of the indices at a given point 
		/// in time.
		/// </summary>
		internal sealed class IndexIntegerList : AbstractBlockIntegerList {
			private readonly IndexSetStore set_store;

			/// <summary>
			/// The number of the index in the store that this list represents.
			/// </summary>
			private readonly int index_num;

			/// <summary>
			/// The maximum block size.
			/// </summary>
			private readonly int max_block_size;

			/// <summary>
			/// Set to true when disposed.
			/// </summary>
			private bool disposed = false;

			/// <summary>
			/// The mapped elements that were deleted.
			/// </summary>
			private readonly ArrayList deleted_blocks = new ArrayList();


			/// <summary>
			/// Constructs the list with the given set of blocks.
			/// </summary>
			/// <param name="set_store"></param>
			/// <param name="index_num"></param>
			/// <param name="max_block_size"></param>
			/// <param name="blocks"></param>
			internal IndexIntegerList(IndexSetStore set_store, int index_num, int max_block_size,
									MappedListBlock[] blocks)
				: base(blocks) {
				this.set_store = set_store;
				this.index_num = index_num;
				this.max_block_size = max_block_size;
			}

			/// <summary>
			/// Creates a new block for the list.
			/// </summary>
			/// <returns></returns>
			protected override IntegerListBlockInterface NewListBlock() {
				if (!disposed) {
					return new MappedListBlock(set_store, max_block_size);
				}
				throw new ApplicationException("Integer list has been disposed.");
			}

			/// <summary>
			/// We must maintain a list of deleted blocks.
			/// </summary>
			/// <param name="list_block"></param>
			protected override void DeleteListBlock(IntegerListBlockInterface list_block) {
				deleted_blocks.Add(list_block);
			}

			/// <summary>
			/// Returns the index number of this list.
			/// </summary>
			public int IndexNumber {
				get { return index_num; }
			}


			/// <summary>
			/// Returns the array of all <see cref="MappedListBlock"/> that are in this list.
			/// </summary>
			public MappedListBlock[] AllBlocks {
				get { return (MappedListBlock[]) block_list.ToArray(typeof (MappedListBlock)); }
			}

			/// <summary>
			/// Returns the array of all <see cref="MappedListBlock"/> that were deleted from 
			/// this list.
			/// </summary>
			public MappedListBlock[] DeletedBlocks {
				get { return (MappedListBlock[]) deleted_blocks.ToArray(typeof (MappedListBlock)); }
			}

			public void Dispose() {
				disposed = true;
				block_list = null;
			}

		}

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
		internal class IndexBlock {
			private readonly IndexSetStore set_store;

			/// <summary>
			/// The number of references to this object.
			/// </summary>
			/// <remarks>
			/// When this reaches 0, it is safe to free any resources that 
			/// this block deleted.
			/// </remarks>
			private int reference_count;

			/// <summary>
			/// The index of this block in the index set.
			/// </summary>
			private readonly int index_num;

			/// <summary>
			/// A pointer that references the area in the store.
			/// </summary>
			private readonly long index_block_p;

			/// <summary>
			/// The total number of entries in the index block.
			/// </summary>
			private readonly long block_entries;

			/// <summary>
			/// The block size of elements in this block.
			/// </summary>
			private readonly int block_size;

			/// <summary>
			/// The list of deleted areas that can safely be disposed when 
			/// this object is disposed.
			/// </summary>
			private ArrayList deleted_areas;

			/// <summary>
			/// True if this block is marked as deleted.
			/// </summary>
			private bool deleted = false;

			/// <summary>
			/// Set to true when this index block is freed from the index store.
			/// </summary>
			private bool freed = false;

			/// <summary>
			/// The parent IndexBlock.  This block is a child modification of the parent.
			/// </summary>
			private IndexBlock parent_block;

			/// <summary>
			/// Constructs the IndexBlock.
			/// </summary>
			/// <param name="set_store"></param>
			/// <param name="index_num"></param>
			/// <param name="block_size"></param>
			/// <param name="index_block_p"></param>
			internal IndexBlock(IndexSetStore set_store, int index_num, int block_size, long index_block_p) {
				this.set_store = set_store;
				this.index_num = index_num;
				this.block_size = block_size;
				this.index_block_p = index_block_p;

				// Read the index count
				IArea index_block_area = set_store.store.GetArea(index_block_p);
				index_block_area.Position = 8;
				block_entries = index_block_area.ReadInt8();

				reference_count = 0;

			}

			/// <summary>
			/// Sets the parent IndexBlock, the index that this index block succeeded.
			/// </summary>
			/// <param name="parent"></param>
			internal void SetParentIndexBlock(IndexBlock parent) {
				this.parent_block = parent;
			}

			/// <summary>
			/// Returns a list of pointers to all mapped blocks.
			/// </summary>
			internal long[] AllBlockPointers {
				get {
					// Create an area for the index block pointer
					IArea index_block_area = set_store.store.GetArea(index_block_p);

					// First create the list of block entries for this list      
					long[] blocks = new long[(int) block_entries];
					if (block_entries != 0) {
						index_block_area.Position = 16;
						for (int i = 0; i < block_entries; ++i) {
							// NOTE: We cast to 'int' here because of internal limitations.
							index_block_area.ReadInt8();
							index_block_area.ReadInt8();
							long element_p = index_block_area.ReadInt8();
							index_block_area.ReadInt4();

							blocks[i] = element_p;
						}
					}

					return blocks;
				}
			}

			/// <summary>
			/// Creates and returns an array of all the <see cref="MappedListBlock"/> objects 
			/// that make up this view of the index integer list.
			/// </summary>
			/// <returns></returns>
			private MappedListBlock[] CreateMappedListBlocks() {
				// Create an area for the index block pointer
				IArea index_block_area = set_store.store.GetArea(index_block_p);
				// First create the list of block entries for this list      
				MappedListBlock[] blocks = new MappedListBlock[(int)block_entries];
				if (block_entries != 0) {
					index_block_area.Position = 16;
					for (int i = 0; i < block_entries; ++i) {
						// NOTE: We cast to 'int' here because of internal limitations.
						long first_entry = index_block_area.ReadInt8();
						long last_entry = index_block_area.ReadInt8();
						long element_p = index_block_area.ReadInt8();
						int type_size = index_block_area.ReadInt4();

						//TODO: check this...
						// size is the first 24 bits (max size = 16MB)
						int element_count = type_size & 0x0FFF;
						byte type = (byte)(ByteBuffer.URShift(type_size, 24) & 0x0F);

						blocks[i] = new MappedListBlock(set_store, first_entry, last_entry, element_p,
														element_count, type, block_size);
					}
				}
				return blocks;
			}

			/// <summary>
			/// Creates and returns a mutable IndexIntegerList object based on this
			/// view of the index.
			/// </summary>
			/// <returns></returns>
			internal IndexIntegerList CreateIndexIntegerList() {
				// Create the MappedListBlock objects for this view
				MappedListBlock[] blocks = CreateMappedListBlocks();
				// And return the IndexIntegerList
				return new IndexIntegerList(set_store, index_num, block_size, blocks);
			}

			/// <summary>
			/// Copies this index block to the given Store and returns a pointer to 
			/// the block within the store.
			/// </summary>
			/// <param name="dest_store"></param>
			/// <returns></returns>
			internal long CopyTo(IStore dest_store) {
				// Create the MappedListBlock object list for this view
				MappedListBlock[] blocks = CreateMappedListBlocks();
				try {
					dest_store.LockForWrite();
					// Create the header area in the store for this block
					IAreaWriter a = dest_store.CreateArea(16 + (blocks.Length * 28));
					long block_p = a.Id;

					a.WriteInt4(1);               // version
					a.WriteInt4(0);               // reserved
					a.WriteInt8(blocks.Length);  // block count
					for (int i = 0; i < blocks.Length; ++i) {
						MappedListBlock entry = blocks[i];
						long b_p = entry.CopyTo(dest_store);
						int block_size = entry.Count;
						a.WriteInt8(entry.first_entry);
						a.WriteInt8(entry.last_entry);
						a.WriteInt8(b_p);
						a.WriteInt4(block_size | (((int)entry.CompactType) << 24));
					}

					// Now finish the area initialization
					a.Finish();

					// Return pointer to the new area in dest_store.
					return block_p;

				} finally {
					dest_store.UnlockForWrite();
				}

			}

			/// <summary>
			/// Recursively calls through the block hierarchy and deletes and 
			/// blocks that can be deleted.
			/// </summary>
			/// <returns></returns>
			private bool DeleteBlockChain() {
				bool parent_deleted = true;
				if (parent_block != null) {
					parent_deleted = parent_block.DeleteBlockChain();
					if (parent_deleted) {
						parent_block = null;
					}
				}

				// If the parent is deleted,
				if (parent_deleted) {
					// Can we delete this block?
					if (reference_count <= 0) {
						if (deleted && deleted_areas != null) {
							set_store.DeleteAllAreas(deleted_areas);
						}
						deleted_areas = null;
					} else {
						// We can't delete this block so return false
						return false;
					}
				}

				return parent_deleted;
			}

			/// <summary>
			/// Adds a reference to this object.
			/// </summary>
			public void AddReference() {
				lock (this) {
					if (freed) {
						throw new Exception("Assertion failed: Block was freed.");
					}
					++reference_count;
				}
			}

			/// <summary>
			/// Removes a reference to this object.
			/// </summary>
			public void RemoveReference() {
				bool pending_delete = false;
				lock (this) {
					--reference_count;
					if (reference_count <= 0) {
						if (freed) {
							throw new Exception(
								  "Assertion failed: remove reference called too many times.");
						}
						if (!deleted && deleted_areas != null) {
							throw new Exception(
								  "Assertion failed: !deleted and deleted_areas != null");
						}
						freed = true;
						if (deleted) {
							AddDeletedArea(index_block_p);
							// Delete these areas
							pending_delete = true;
						}
					}
				} // lock(this)
				if (pending_delete) {
					lock (set_store) {
						DeleteBlockChain();
					}
				}
			}

			/// <summary>
			/// Returns the number of references to this object.
			/// </summary>
			public int ReferenceCount {
				get {
					lock (this) {
						return reference_count;
					}
				}
			}

			/// <summary>
			/// Returns the block size that has been set on this list.
			/// </summary>
			public int BlockSize {
				get { return block_size; }
			}

			/// <summary>
			/// Returns the pointer to this index block in the store.
			/// </summary>
			public long Pointer {
				get { return index_block_p; }
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
					if (deleted_areas == null) {
						deleted_areas = new ArrayList();
					}

					deleted_areas.Add(pointer);

				}
			}
		}
	}
}