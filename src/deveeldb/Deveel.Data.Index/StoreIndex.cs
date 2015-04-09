using System;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Store;
using Deveel.Data.Util;

namespace Deveel.Data.Index {
	class StoreIndex : BlockIndex<int>, IIndex, IDisposable {
		private List<IMappedBlock> deletedBlocks;

		private bool disposed;

		public StoreIndex(IndexSetStore indexSetStore, int indexNumber, int maxBlockSize, IEnumerable<IIndexBlock<int>> blocks)
			: base(blocks) {
			IndexSetStore = indexSetStore;
			IndexNumber = indexNumber;
			MaxBlockSize = maxBlockSize;

			deletedBlocks = new List<IMappedBlock>();
		}


		public int IndexNumber { get; private set; }

		public int MaxBlockSize { get; private set; }

		public IndexSetStore IndexSetStore { get; private set; }

		public IEnumerable<IIndexBlock<int>> AllBlocks {
			get { return Blocks.AsReadOnly(); }
		}

		public IEnumerable<IMappedBlock> DeletedBlocks {
			get { return deletedBlocks.AsReadOnly(); }
		}

		private void AssertNotDisposed() {
			if (disposed)
				throw new ObjectDisposedException(GetType().FullName);
		}

		protected override IIndexBlock<int> NewBlock() {
			AssertNotDisposed();

			return new MappedBlock(this);
		}

		protected override void OnDeleteBlock(IIndexBlock<int> block) {
			deletedBlocks.Add((IMappedBlock) block);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (!disposed) {
				if (disposing) {
					IndexSetStore = null;

					if (deletedBlocks != null)
						deletedBlocks.Clear();

					deletedBlocks = null;
				}

				disposed = true;
			}
		}

		public static IMappedBlock NewMappedBlock(IndexSetStore indexSetStore, int firstEntry, int lastEntry, long blockPointer,
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

			public MappedBlock(StoreIndex index)
				: base(index.MaxBlockSize) {
				IndexSetStore = index.IndexSetStore;
				maxBlockSize = index.MaxBlockSize;
			}

			public MappedBlock(IndexSetStore indexSetStore, int firstEntry, int lastEntry, long blockPointer, int size, byte compactType, int maxBlockSize) {
				IndexSetStore = indexSetStore;
				FirstEntry = firstEntry;
				LastEntry = lastEntry;
				BlockPointer = blockPointer;
				CompactType = compactType;

				this.maxBlockSize = maxBlockSize;
				Count = size;
				BaseArray = null;
			}

			public int FirstEntry { get; private set; }

			public int LastEntry { get; private set; }

			public long BlockPointer { get; private set; }

			public byte CompactType { get; private set; }

			public override int Top {
				get {
					if (Count == 0)
						throw new ApplicationException("No first int in block.");

					lock (blockLock) {
						return BaseArray == null ? LastEntry : BaseArray[Count - 1];
					}
				}
			}

			public override int Bottom {
				get {
					if (Count == 0)
						throw new ApplicationException("No first int in block.");

					lock (blockLock) {
						return BaseArray == null ? FirstEntry : BaseArray[0];
					}
				}
			}

			protected override int ArrayLength {
				get { return maxBlockSize; }
			}

			private void PrepareMutate(bool readOnly) {
				// If list is to be mutable
				if (!readOnly && !mutableBlock) {
					BaseArray = (int[])BaseArray.Clone();
					mutableBlock = true;
				}
			}

			public long CopyTo(IStore destStore) {
				// The number of bytes per entry
				int entrySize = CompactType;
				// The total size of the entry.
				int areaSize = (Count * entrySize);

				// Allocate the destination area
				var dest = destStore.CreateArea(areaSize);
				long destOffset = dest.Id;
				Store.GetArea(BlockPointer).CopyTo(dest, areaSize);
				dest.Flush();

				return destOffset;
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
				int areaSize = (Count * entrySize);

				// Allocate an array to buffer the block to
				byte[] arr = new byte[areaSize];
				// Fill the array
				int p = 0;
				for (int i = 0; i < Count; ++i) {
					int v = BaseArray[i];
					for (int n = entrySize - 1; n >= 0; --n) {
						//TODO: check this...
						arr[p] = (byte)(ByteBuffer.URShift(v, (n * 8)) & 0x0FF);
						++p;
					}
				}

				// Create an area to store this
				var a = Store.CreateArea(areaSize);
				BlockPointer = a.Id;

				a.Write(arr, 0, areaSize);
				a.Flush();

				// Once written, the block is invalidated
				blockLock = null;

				return BlockPointer;
			}

			protected override int[] GetArray(bool readOnly) {
				// We must synchronize this entire block because otherwise we could
				// return a partially loaded array.
				lock (blockLock) {
					if (BaseArray != null) {
						PrepareMutate(readOnly);
						return BaseArray;
					}

					// Create the int array
					BaseArray = new int[maxBlockSize];

					// The number of bytes per entry
					int entrySize = CompactType;
					// The total size of the entry.
					int areaSize = (Count * entrySize);

					// Read in the byte array
					byte[] buf = new byte[areaSize];
					try {
						Store.GetArea(BlockPointer).Read(buf, 0, areaSize);
					} catch (IOException e) {
						throw new ApplicationException("IO Error: " + e.Message);
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
}