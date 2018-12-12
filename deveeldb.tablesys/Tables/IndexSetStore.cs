using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Deveel.Collections;
using Deveel.Data.Sql.Indexes;
using Deveel.Data.Sql.Util;
using Deveel.Data.Storage;

namespace Deveel.Data.Sql.Tables
{
	public sealed class IndexSetStore : IDisposable {
		private IArea startArea;

		private long indexHeaderPointer;
		private IArea indexHeaderArea;

		private IndexBlock[] indexBlocks;

		// private bool disposed;

		private const int Magic = 0x0CA90291;

		public IndexSetStore(IStore store) {
			if (store == null)
				throw new ArgumentNullException(nameof(store));

			Store = store;
		}

		public IStore Store { get; private set; }

		internal void DeleteAreas(IEnumerable<int> deletedAreas) {
			lock (this) {
				if (Store != null) {
					try {
						Store.Lock();

						foreach (var id in deletedAreas) {
							Store.DeleteArea(id);
						}

					} catch (IOException) {
						// TODO: Raise the error to the system
					} finally {
						Store.Unlock();
					}
				}
			}
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (indexHeaderArea != null)
					indexHeaderArea.Dispose();

				if (startArea != null)
					startArea.Dispose();
			}

			indexHeaderArea = null;
			Store = null;
			startArea = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private long CreateNewBlock() {
			// Allocate the area
			using (var a = Store.CreateArea(16)) {
				long indexBlockP = a.Id;
				// Setup the header
				a.Write(1); // version
				a.Write(0); // reserved
				a.Write(0L); // block entries
				a.Flush();

				return indexBlockP;
			}
		}

		public long Create() {
			lock (this) {
				// Create an empty index header area
				using (var a = Store.CreateArea(16)) {
					indexHeaderPointer = a.Id;
					a.Write(1); // version
					a.Write(0); // reserved
					a.Write(0L); // number of indexes in the set
					a.Flush();
				}

				// Set up the local IArea object for the index header
				indexHeaderArea = Store.GetArea(indexHeaderPointer);

				indexBlocks = new IndexBlock[0];

				// Allocate the starting header
				using (var sa = Store.CreateArea(32)) {
					long startPointer = sa.Id;
					// The magic
					sa.Write(Magic);
					// The version
					sa.Write(1);
					// Pointer to the index header
					sa.Write(indexHeaderPointer);
					sa.Flush();

					// Set the 'start_area' value.
					startArea = Store.GetArea(startPointer);

					return startPointer;
				}
			}
		}

		public void Open(long startPointer) {
			lock (this) {
				// Set up the start area
				startArea = Store.GetArea(startPointer);

				int magic = startArea.ReadInt32();
				if (magic != Magic)
					throw new IOException("Magic value for index set does not match.");

				int version = startArea.ReadInt32();
				if (version != 1)
					throw new IOException("Unknown version for index set.");

				// Setup the index_header area
				indexHeaderPointer = startArea.ReadInt64();
				indexHeaderArea = Store.GetArea(indexHeaderPointer);

				// Read the index header area
				version = indexHeaderArea.ReadInt32(); // version
				if (version != 1)
					throw new IOException("Incorrect version");

				indexHeaderArea.ReadInt32(); // reserved
				int indexCount = (int)indexHeaderArea.ReadInt64();
				indexBlocks = new IndexBlock[indexCount];

				// Initialize each index block
				for (int i = 0; i < indexCount; ++i) {
					int type = indexHeaderArea.ReadInt32();
					int blockSize = indexHeaderArea.ReadInt32();
					long indexBlockPointer = indexHeaderArea.ReadInt64();
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

		public void GetAreasUsed(BigList<long> list) {
			list.Add(startArea.Id);
			list.Add(indexHeaderPointer);

			foreach (var block in indexBlocks) {
				list.Add(block.StartOffset);
				long[] blockPointers = block.GetBlockPointers();
				list.AddRange(blockPointers);
			}
		}

		public void PrepareIndexes(int count, int type, int blockSize) {
			lock (this) {
				try {
					Store.Lock();

					// Allocate a new area for the list
					int newSize = 16 + ((indexBlocks.Length + count) * 16);
					using (var newIndexArea = Store.CreateArea(newSize)) {
						long newIndexPointer = newIndexArea.Id;
						var newIndexBlocks = new IndexBlock[(indexBlocks.Length + count)];

						// Copy the existing area
						indexHeaderArea.Position = 0;
						int version = indexHeaderArea.ReadInt32();
						int reserved = indexHeaderArea.ReadInt32();
						long icount = indexHeaderArea.ReadInt64();
						newIndexArea.Write(version);
						newIndexArea.Write(reserved);
						newIndexArea.Write(icount + count);

						for (int i = 0; i < indexBlocks.Length; ++i) {
							int itype = indexHeaderArea.ReadInt32();
							int iblockSize = indexHeaderArea.ReadInt32();
							long indexBlockP = indexHeaderArea.ReadInt64();

							newIndexArea.Write(itype);
							newIndexArea.Write(iblockSize);
							newIndexArea.Write(indexBlockP);

							newIndexBlocks[i] = indexBlocks[i];
						}

						// Add the new entries
						for (int i = 0; i < count; ++i) {
							long newBlankBlockP = CreateNewBlock();

							newIndexArea.Write(type);
							newIndexArea.Write(blockSize);
							newIndexArea.Write(newBlankBlockP);

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
						startArea.Write(newIndexPointer);
						startArea.Flush();

						// Free the old header
						Store.DeleteArea(oldIndexHeaderP);
					}
				} finally {
					Store.Unlock();
				}
			}
		}

		public IRowIndexSet GetSnapshotIndex() {
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
				long aOffset;
				using (var a = Store.CreateArea(16 + (indexBlocks.Length * 16))) {
					aOffset = a.Id;

					a.Write(1); // version
					a.Write(0); // reserved
					a.Write((long) indexBlocks.Length); // count

					foreach (var indBlock in indexBlocks) {
						a.Write(1);
						a.Write(indBlock.BlockSize);
						a.Write(indBlock.StartOffset);
					}

					// Finish creating the updated header
					a.Flush();
				}

				// The old index header pointer
				long oldIndexHeaderP = indexHeaderPointer;

				// Set the new index header
				indexHeaderPointer = aOffset;
				indexHeaderArea = Store.GetArea(indexHeaderPointer);

				// Write the change to 'startPointer'
				startArea.Position = 8;
				startArea.Write(indexHeaderPointer);
				startArea.Flush();

				// Free the old header index
				Store.DeleteArea(oldIndexHeaderP);
			}
		}

		public void CommitIndexSet(IRowIndexSet indexSet) {
			var removedBlocks = new List<IndexBlock>();

			lock (this) {
				var sIndexSet = (SnapshotIndexSet)indexSet;

				try {
					try {
						Store.Lock();

						// For each Index in the index set,
						foreach (Index index in sIndexSet) {
							int indexNum = index.IndexNumber;

							// The IndexBlock we are changing
							var curIndexBlock = indexBlocks[indexNum];

							// Get all the blocks in the list
							var blocks = index.AllBlocks.ToList();

							// Make up a new block list for this index set.
							long blockP;
							using (var area = Store.CreateArea(16 + (blocks.Count * 28))) {
								blockP = area.Id;
								area.Write(1); // version
								area.Write(0); // reserved
								area.Write((long)blocks.Count); // block count

								foreach (var block in blocks) {
									var mappedBlock = (IMappedBlock)block;

									long bottomInt = 0;
									long topInt = 0;
									var blockSize = mappedBlock.Count;
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

									area.Write(bottomInt);
									area.Write(topInt);
									area.Write(blockPointer);
									area.Write((int)(blockSize | ((mappedBlock.CompactType) << 24)));
								}

								// Finish initializing the area
								area.Flush();
							}

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
						Store.Unlock();
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
					Store.Lock();

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
					Store.Unlock();
				}
			}
		}

		#region SnapshotIndexSet

		class SnapshotIndexSet : IRowIndexSet {
			private readonly IndexSetStore indexSetStore;
			private List<Index> indexes;
			private IndexBlock[] indexBlocks;

			private bool disposed;

			private static readonly Index[] EmptyIndex = new Index[0];

			public SnapshotIndexSet(IndexSetStore indexSetStore, IndexBlock[] blocks) {
				this.indexSetStore = indexSetStore;
				indexBlocks = blocks;

				// Not disposed.
				disposed = false;

			}

			~SnapshotIndexSet() {
				Dispose(false);
			}


			public IEnumerator<IRowIndex> GetEnumerator() {
				if (indexes == null)
					return EmptyIndex.Cast<IRowIndex>().GetEnumerator();

				return indexes.GetEnumerator();
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			public IRowIndex GetIndex(int offset) {
				// Create if not exist.
				if (indexes == null) {
					indexes = new List<Index>();
				} else {
					// If this list has already been created, return it
					foreach (var index in indexes) {
						if (index.IndexNumber == offset)
							return index;
					}
				}

				try {
					var index = (Index)indexBlocks[offset].CreateIndex();
					indexes.Add(index);
					return index;
				} catch (IOException e) {
					throw new Exception("IO Error: " + e.Message, e);
				}
			}

			public void Dispose() {
				Dispose(true);
				GC.SuppressFinalize(this);
			}

			private void Dispose(bool disposing) {
				if (!disposed) {
					if (disposing) {
						try {
							if (indexes != null) {
								foreach (var index in indexes) {
									index.Dispose();
								}
							}

							// Release reference to the index_blocks;
							foreach (var block in indexBlocks) {
								block.RemoveReference();
							}
						} catch (Exception) {
						}
					}

					indexes = null;
					indexBlocks = null;
					disposed = true;
				}
			}
		}

		#endregion

		#region Index

		class Index : SortedCollection<SqlObject, long>, IRowIndex, IDisposable {
			private List<IMappedBlock> deletedBlocks;

			private bool disposed;

			public Index(IndexSetStore indexSetStore, int indexNumber, int maxBlockSize, IEnumerable<ICollectionBlock<SqlObject, long>> blocks)
				: base(blocks) {
				IndexSetStore = indexSetStore;
				IndexNumber = indexNumber;
				MaxBlockSize = maxBlockSize;

				deletedBlocks = new List<IMappedBlock>();
			}

			public int IndexNumber { get; }

			public int MaxBlockSize { get; private set; }

			public IndexSetStore IndexSetStore { get; private set; }

			public IEnumerable<ICollectionBlock<SqlObject, long>> AllBlocks {
				get { return Blocks.ToArray(); }
			}

			public IEnumerable<IMappedBlock> DeletedBlocks {
				get { return deletedBlocks.ToArray(); }
			}

			private void AssertNotDisposed() {
				if (disposed)
					throw new ObjectDisposedException(GetType().FullName);
			}

			protected override ICollectionBlock<SqlObject, long> NewBlock() {
				AssertNotDisposed();

				return new MappedBlock(this);
			}

			protected override void OnDeleteBlock(ICollectionBlock<SqlObject, long> block) {
				deletedBlocks.Add((IMappedBlock)block);
			}


			public void Dispose() {
				Dispose(true);
				GC.SuppressFinalize(this);
			}

			private void Dispose(bool disposing) {
				if (!disposed) {
					if (disposing) {
						if (deletedBlocks != null)
							deletedBlocks.Clear();
					}

					IndexSetStore = null;
					deletedBlocks = null;
					disposed = true;
				}
			}

			public static IMappedBlock NewMappedBlock(IndexSetStore indexSetStore, long firstEntry, long lastEntry, long blockPointer,
				int size, byte compactType, int blockSize) {
				return new MappedBlock(indexSetStore, firstEntry, lastEntry, blockPointer, size, compactType, blockSize);
			}

			#region MappedBlock

			class MappedBlock : Block, IMappedBlock, IDisposable {
				private readonly int maxBlockSize;

				private object blockLock = new object();
				private bool mutableBlock;


				public IndexSetStore IndexSetStore { get; private set; }

				private IStore Store {
					get { return IndexSetStore.Store; }
				}

				public MappedBlock(Index index)
					: base(index.MaxBlockSize) {
					IndexSetStore = index.IndexSetStore;
					maxBlockSize = index.MaxBlockSize;
					BlockPointer = -1;
				}

				public MappedBlock(IndexSetStore indexSetStore, long firstEntry, long lastEntry, long blockPointer, int size, byte compactType, int maxBlockSize) {
					IndexSetStore = indexSetStore;
					FirstEntry = firstEntry;
					LastEntry = lastEntry;
					BlockPointer = blockPointer;
					CompactType = compactType;

					this.maxBlockSize = maxBlockSize;
					Count = size;
					BaseArray = null;
				}

				public long FirstEntry { get; private set; }

				public long LastEntry { get; private set; }

				public long BlockPointer { get; private set; }

				public byte CompactType { get; private set; }

				public override long Top {
					get {
						if (Count == 0)
							throw new InvalidOperationException("No first int in block.");

						lock (blockLock) {
							return BaseArray == null ? (int)LastEntry : BaseArray[Count - 1];
						}
					}
				}

				public override long Bottom {
					get {
						if (Count == 0)
							throw new InvalidOperationException("No first int in block.");

						lock (blockLock) {
							return BaseArray == null ? (int)FirstEntry : BaseArray[0];
						}
					}
				}

				protected override long ArrayLength => maxBlockSize;

				private void PrepareMutate(bool readOnly) {
					// If list is to be mutable
					if (!readOnly && !mutableBlock) {
						BaseArray = (BigArray<long>)BaseArray.Clone();
						mutableBlock = true;
					}
				}

				public long CopyTo(IStore destStore) {
					// The number of bytes per entry
					int entrySize = CompactType;
					// The total size of the entry.
					var areaSize = (Count * entrySize);

					// Allocate the destination area
					using (var dest = destStore.CreateArea(areaSize)) {
						long destOffset = dest.Id;
						using (var source = Store.GetArea(BlockPointer)) {
							source.CopyTo(dest, (int) areaSize);
							dest.Flush();
						}

						return destOffset;
					}
				}

				public long Flush() {
					// Convert the int[] array to a byte[] array.

					// First determine how we compact this int array into a byte array.  If
					// all the values are < 32768 then we store as shorts
					long largestVal = 0;
					for (int i = 0; i < Count; ++i) {
						long v = BaseArray[i];
						if (System.Math.Abs(v) > System.Math.Abs(largestVal)) {
							largestVal = v;
						}
					}

					long lv = largestVal;
					if (lv >> 7 == 0 || lv >> 7 == -1) {
						CompactType = 1;
					} else if (lv >> 15 == 0 || lv >> 15 == -1) {
						CompactType = 2;
					} else if (lv >> 23 == 0 || lv >> 23 == -1) {
						CompactType = 3;
					}
					// NOTE: in the future we'll want to determine if we are going to store
					//   as an int or long array.
					else {
						CompactType = 4;
					}

					// The number of bytes per entry
					int entrySize = CompactType;
					// The total size of the entry.
					var areaSize = (Count * entrySize);

					// Allocate an array to buffer the block to
					byte[] arr = new byte[areaSize];
					// Fill the array
					int p = 0;
					for (int i = 0; i < Count; ++i) {
						var v = BaseArray[i];
						for (int n = entrySize - 1; n >= 0; --n) {
							//TODO: check this...
							arr[p] = (byte)(BytesUtil.URShift((int) v, (n * 8)) & 0x0FF);
							++p;
						}
					}

					// Create an area to store this
					using (var a = Store.CreateArea(areaSize)) {
						BlockPointer = a.Id;

						a.Write(arr, 0, (int) areaSize);
						a.Flush();
					}

					// Once written, the block is invalidated
					blockLock = null;

					return BlockPointer;
				}

				protected override BigArray<long> GetArray(bool readOnly) {
					// We must synchronize this entire block because otherwise we could
					// return a partially loaded array.
					lock (blockLock) {
						if (BaseArray != null) {
							PrepareMutate(readOnly);
							return BaseArray;
						}

						// Create the int array
						BaseArray = new BigArray<long>(maxBlockSize);

						// The number of bytes per entry
						int entrySize = CompactType;
						// The total size of the entry.
						var areaSize = (Count * entrySize);

						// Read in the byte array
						byte[] buf = new byte[areaSize];
						try {
							Store.GetArea(BlockPointer).Read(buf, 0, (int) areaSize);
						} catch (IOException e) {
							throw new InvalidOperationException("IO Error: " + e.Message);
						}

						// Uncompact it into the int array
						int p = 0;
						for (int i = 0; i < Count; ++i) {
							int v = (((int)buf[p]) << ((entrySize - 1) * 8));
							++p;
							for (int n = entrySize - 2; n >= 0; --n) {
								v = v | ((((int)buf[p]) & 0x0FF) << (n * 8));
								++p;
							}
							BaseArray[i] = v;
						}

						mutableBlock = false;
						PrepareMutate(readOnly);
						return BaseArray;
					}
				}

				public void Dispose() {
					IndexSetStore = null;
				}
			}

			#endregion
		}

		#endregion

		#region IndexBlock

		public sealed class IndexBlock {
			private readonly IndexSetStore indexSetStore;
			private readonly int indexNum;
			private readonly long blockEntries;

			private List<int> deletedAreas;

			private int refCount;

			internal IndexBlock(IndexSetStore indexSetStore, int indexNum, int blockSize, long startOffset) {
				this.indexSetStore = indexSetStore;
				this.indexNum = indexNum;
				BlockSize = blockSize;
				StartOffset = startOffset;

				// Read the index count
				var indexBlockArea = indexSetStore.Store.GetArea(startOffset);
				indexBlockArea.Position = 8;
				blockEntries = indexBlockArea.ReadInt64();

				refCount = 0;
			}

			public IndexBlock Parent { get; set; }

			public bool IsFreed { get; private set; }

			public bool IsDeleted { get; private set; }

			public int BlockSize { get; }

			public long StartOffset { get; }

			private IEnumerable<IMappedBlock> CreateMappedBlocks() {
				// Create an area for the index block pointer
				var indexBlockArea = indexSetStore.Store.GetArea(StartOffset);

				// First create the list of block entries for this list      
				var blocks = new IMappedBlock[(int)blockEntries];
				if (blockEntries != 0) {
					indexBlockArea.Position = 16;
					for (int i = 0; i < blockEntries; ++i) {
						// NOTE: We cast to 'int' here because of internal limitations.
						var firstEntry = indexBlockArea.ReadInt64();
						var lastEntry = indexBlockArea.ReadInt64();
						var blockPointer = indexBlockArea.ReadInt64();
						var typeSize = indexBlockArea.ReadInt32();

						//TODO: check this...
						// size is the first 24 bits (max size = 16MB)
						int elementCount = typeSize & 0x0FFF;
						byte type = (byte)(BytesUtil.URShift(typeSize, 24) & 0x0F);

						blocks[i] = Index.NewMappedBlock(indexSetStore, firstEntry, lastEntry, blockPointer,
							elementCount, type, BlockSize);
					}
				}

				return blocks;
			}

			public ISortedCollection<SqlObject, long> CreateIndex() {
				// Create the MappedListBlock objects for this view
				var blocks = CreateMappedBlocks().Cast<ICollectionBlock<SqlObject, long>>();
				// And return the Index
				return new Index(indexSetStore, indexNum, BlockSize, blocks);
			}

			internal long[] GetBlockPointers() {
				// Create an area for the index block pointer
				var indexBlockArea = indexSetStore.Store.GetArea(StartOffset);

				// First create the list of block entries for this list      
				long[] blocks = new long[(int)blockEntries];
				if (blockEntries != 0) {
					indexBlockArea.Position = 16;

					for (int i = 0; i < blockEntries; ++i) {
						// NOTE: We cast to 'int' here because of internal limitations.
						indexBlockArea.ReadInt64();
						indexBlockArea.ReadInt64();
						long elementP = indexBlockArea.ReadInt64();
						indexBlockArea.ReadInt32();

						blocks[i] = elementP;
					}
				}

				return blocks;
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

		#endregion
	}
}
