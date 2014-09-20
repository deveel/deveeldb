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
using System.IO;

using Deveel.Data.Store;
using Deveel.Data.Util;
using Deveel.Diagnostics;

using SysMath = System.Math;

namespace Deveel.Data.Index {
	sealed partial class IndexSetStore {
		/// <summary>
		/// The <see cref="IIndex"/> implementation that is used to 
		/// represent a mutable snapshop of the indices at a given point 
		/// in time.
		/// </summary>
		sealed class Index : BlockIndex {
			private readonly IndexSetStore store;

			/// <summary>
			/// The number of the index in the store that this list represents.
			/// </summary>
			private readonly int indexNum;

			/// <summary>
			/// The maximum block size.
			/// </summary>
			private readonly int maxBlockSize;

			/// <summary>
			/// Set to true when disposed.
			/// </summary>
			private bool disposed;

			/// <summary>
			/// The mapped elements that were deleted.
			/// </summary>
			private readonly List<IMappedBlock> deletedBlocks = new List<IMappedBlock>();


			/// <summary>
			/// Constructs the list with the given set of blocks.
			/// </summary>
			/// <param name="store"></param>
			/// <param name="indexNum"></param>
			/// <param name="maxBlockSize"></param>
			/// <param name="blocks"></param>
			internal Index(IndexSetStore store, int indexNum, int maxBlockSize, IIndexBlock[] blocks)
				: base(blocks) {
				this.store = store;
				this.indexNum = indexNum;
				this.maxBlockSize = maxBlockSize;
			}

			protected override IIndexBlock NewBlock() {
				if (disposed)
					throw new ObjectDisposedException("IndexSetStore.Index");

				return new MappedBlock(store, maxBlockSize);
			}

			protected override void OnDeleteBlock(IIndexBlock block) {
				deletedBlocks.Add((IMappedBlock)block);
			}

			/// <summary>
			/// Returns the index number of this list.
			/// </summary>
			public int IndexNumber {
				get { return indexNum; }
			}


			/// <summary>
			/// Returns the array of all <see cref="MappedBlock"/> that are in this list.
			/// </summary>
			public IList<IIndexBlock> AllBlocks {
				get { return Blocks; }
			}

			/// <summary>
			/// Returns the array of all <see cref="MappedBlock"/> that were deleted from 
			/// this list.
			/// </summary>
			public IMappedBlock[] DeletedBlocks {
				get { return deletedBlocks.ToArray(); }
			}

			public void Dispose() {
				disposed = true;
				// block_list = null;
			}

			/// <summary>
			/// An <see cref="IIndexBlock"/> implementation that maps a block 
			/// of a list to an underlying file system representation.
			/// </summary>
			class MappedBlock : Block, IMappedBlock {
				private readonly IndexSetStore store;

				/// <summary>
				/// The first entry in the block.
				/// </summary>
				private readonly long firstEntry;

				/// <summary>
				/// The last entry in the block.
				/// </summary>
				private readonly long lastEntry;

				/// <summary>
				/// A pointer to the area where this block can be found.
				/// </summary>
				private long blockPointer;

				/// <summary>
				/// Lock object.
				/// </summary>
				private object blockLock = new object();

				/// <summary>
				/// Set to true if the loaded block is mutable.
				/// </summary>
				private bool mutableBlock;

				/// <summary>
				/// How this block is compacted in the store.  If this is 1 the elements 
				/// are stored as shorts, if it is 2 - ints, and if it is 3 - longs.
				/// </summary>
				private byte compactType;

				/// <summary>
				/// The maximum size of the block.
				/// </summary>
				private readonly int maxBlockSize;

				public MappedBlock(IndexSetStore store, long firstEntry, long lastEntry, long blockPointer, int size, byte compactType, int maxBlockSize) {
					this.store = store;
					this.firstEntry = firstEntry;
					this.lastEntry = lastEntry;
					this.blockPointer = blockPointer;
					this.compactType = compactType;
					this.maxBlockSize = maxBlockSize;
					Count = size;
					BaseArray = null;
				}

				/// <summary>
				/// Creates an empty block.
				/// </summary>
				/// <param name="store"></param>
				/// <param name="maxBlockSize"></param>
				public MappedBlock(IndexSetStore store, int maxBlockSize)
					: base(maxBlockSize) {
					this.store = store;
					blockPointer = -1;
					this.maxBlockSize = maxBlockSize;
				}

				public long FirstEntry {
					get { return firstEntry; }
				}

				public long LastEntry {
					get { return lastEntry; }
				}

				/// <summary>
				/// Returns a pointer to the area that contains this block.
				/// </summary>
				public long BlockPointer {
					get { return blockPointer; }
				}

				/// <summary>
				/// Returns the compact type of this block.
				/// </summary>
				public byte CompactType {
					get { return compactType; }
				}

				/// <summary>
				/// Copies the index data in this block to a new block in the given store
				/// and returns a pointer to the new block.
				/// </summary>
				/// <param name="destStore"></param>
				/// <returns></returns>
				public long CopyTo(IStore destStore) {
					// The number of bytes per entry
					int entrySize = compactType;
					// The total size of the entry.
					int areaSize = (Count * entrySize);

					// Allocate the destination area
					IAreaWriter dest = destStore.CreateArea(areaSize);
					long destBlockP = dest.Id;
					store.store.GetArea(blockPointer).CopyTo(dest, areaSize);
					dest.Finish();

					return destBlockP;
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
					long largestVal = 0;
					for (int i = 0; i < Count; ++i) {
						long v = BaseArray[i];
						if (SysMath.Abs(v) > SysMath.Abs(largestVal)) {
							largestVal = v;
						}
					}

					long lv = largestVal;
					if (lv >> 7 == 0 || lv >> 7 == -1) {
						compactType = 1;
					} else if (lv >> 15 == 0 || lv >> 15 == -1) {
						compactType = 2;
					} else if (lv >> 23 == 0 || lv >> 23 == -1) {
						compactType = 3;
					}
						// NOTE: in the future we'll want to determine if we are going to store
						//   as an int or long array.
					  else {
						compactType = 4;
					}

					// The number of bytes per entry
					int entrySize = compactType;
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
					IAreaWriter a = store.store.CreateArea(areaSize);
					blockPointer = a.Id;
					// Write to the area
					a.Write(arr, 0, areaSize);
					// And finish the area initialization
					a.Finish();

					// Once written, the block is invalidated
					blockLock = null;

					return blockPointer;
				}

				/// <summary>
				/// Overwritten from <see cref="Block"/>, this returns the 
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
					lock (blockLock) {
						if (BaseArray != null) {
							PrepareMutate(immutable);
							return BaseArray;
						}

						// Create the int array
						BaseArray = new int[maxBlockSize];

						// The number of bytes per entry
						int entrySize = compactType;
						// The total size of the entry.
						int areaSize = (Count * entrySize);

						// Read in the byte array
						byte[] buf = new byte[areaSize];
						try {
							store.store.GetArea(blockPointer).Read(buf, 0, areaSize);
						} catch (IOException e) {
							store.context.Logger.Error(this, "blockPointer = " + blockPointer);
							store.context.Logger.Error(this, e);
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
						PrepareMutate(immutable);
						return BaseArray;

					}

				}

				/// <inheritdoc/>
				protected override int ArrayLength {
					get { return maxBlockSize; }
				}

				/// <summary>
				/// Makes the block mutable if it is immutable.
				/// </summary>
				/// <param name="immutable"></param>
				/// <remarks>
				/// We must be synchronized on <see cref="blockLock"/> before this 
				/// method is called.
				/// </remarks>
				private void PrepareMutate(bool immutable) {
					// If list is to be mutable
					if (!immutable && !mutableBlock) {
						BaseArray = (int[])BaseArray.Clone();
						mutableBlock = true;
					}
				}

				/// <inheritdoc/>
				public override int Top {
					get {
						if (Count == 0)
							throw new ApplicationException("No first int in block.");

						lock (blockLock) {
							return BaseArray == null ? (int) lastEntry : BaseArray[Count - 1];
						}
					}
				}

				/// <inheritdoc/>
				public override int Bottom {
					get {
						if (Count == 0)
							throw new ApplicationException("No first int in block.");

						lock (blockLock) {
							return BaseArray == null ? (int) firstEntry : BaseArray[0];
						}
					}
				}
			}

			internal static IMappedBlock NewMappedListBlock(IndexSetStore store, long firstEntry, long lastEntry, long blockPointer, int size, byte compactType, int blockSize) {
				return new MappedBlock(store, firstEntry, lastEntry, blockPointer, size, compactType, blockSize);
			}
		}

		/// <summary>
		/// A convenience static empty integer list array.
		/// </summary>
		private static readonly Index[] EmptyIndex = new Index[0];

		interface IMappedBlock : IIndexBlock {
			long FirstEntry { get; }

			long LastEntry { get; }

			long BlockPointer { get; }

			byte CompactType { get; }


			long CopyTo(IStore destStore);

			long WriteToStore();
		}
	}
}