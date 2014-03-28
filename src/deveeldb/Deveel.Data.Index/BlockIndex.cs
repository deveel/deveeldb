// 
//  Copyright 2011  Deveel
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
using System.Collections.Generic;

namespace Deveel.Data.Index {
	///<summary>
	/// An implementation of <see cref="BlockIndexBase"/> that stores 
	/// all values in blocks that are entirely stored in main memory.
	///</summary>
	/// <remarks>
	/// This type of structure is useful for large in-memory lists in which a
	/// dd/remove performance must be fast.
	/// </remarks>
	public class BlockIndex : BlockIndexBase {
		public BlockIndex() {
		}

		public BlockIndex(IEnumerable<int> values)
			: base(values) {
		}

		public BlockIndex(IIndex index)
			: base(index) {
		}

		protected BlockIndex(IEnumerable<IBlockIndexBlock> blocks)
			: base(blocks) {
		}

		protected override IBlockIndexBlock NewBlock() {
			return new Block(512);
		}

		#region Block

		protected class Block : IBlockIndexBlock {
			private Block next;
			private Block prev;
			private int[] array;
			private int count;
			private bool changed;

			protected Block() {	
			}

			public Block(int blockSize)
				: this() {
				array = new int[blockSize];
				count = 0;
			}

			protected int[] BaseArray {
				get { return array; }
				set { array = value; }
			}

			protected virtual int ArrayLength {
				get { return array.Length; }
			}

			public IEnumerator<int> GetEnumerator() {
				return new Enumerator(this);
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			IBlockIndexBlock IBlockIndexBlock.Next {
				get { return Next; }
				set { Next = (Block) value; }
			}

			public Block Next {
				get { return next; }
				set { next = value; }
			}

			IBlockIndexBlock IBlockIndexBlock.Previous {
				get { return Previous; }
				set { Previous = (Block) value; }
			}

			public Block Previous {
				get { return prev; }
				set { prev = value; }
			}

			public bool HasChanged {
				get { return changed; }
			}

			public int Count {
				get { return count; }
				protected set { count = value; }
			}

			public bool IsFull {
				get { return count >= ArrayLength; }
			}

			public bool IsEmpty {
				get { return count <= 0; }
			}

			public virtual int Top {
				get { return GetArray(true)[count - 1]; }
			}

			public virtual int Bottom {
				get {
					if (count <= 0)
						throw new ApplicationException("no bottom value.");

					return GetArray(true)[0];
				}
			}

			public int this[int index] {
				get { return GetArray(true)[index]; }
				set { 
					changed = true;
					GetArray(false)[index] = value;
				}
			}

			protected virtual int[] GetArray(bool readOnly) {
				return array;
			}

			public bool CanContain(int number) {
				return count + number + 1 < ArrayLength;
			}

			public void Add(int value) {
				changed = true;
				int[] arr = GetArray(false);
				arr[count] = value;
				++count;
			}

			public int RemoveAt(int index) {
				changed = true;
				int[] arr = GetArray(false);
				int val = arr[index];
				Array.Copy(array, index + 1, arr, index, (count - index));
				--count;
				return val;
			}

			public int IndexOf(int value) {
				int[] arr = GetArray(true);
				for (int i = count - 1; i >= 0; --i) {
					if (arr[i] == value)
						return i;
				}
				return -1;
			}

			public int IndexOf(int value, int startIndex) {
				int[] arr = GetArray(true);
				for (int i = startIndex; i < count; ++i) {
					if (arr[i] == value)
						return i;
				}
				return -1;
			}

			public void Insert(int index, int value) {
				changed = true;
				int[] arr = GetArray(false);
				Array.Copy(array, index, arr, index + 1, (count - index));
				++count;
				arr[index] = value;
			}

			public void MoveTo(IBlockIndexBlock destBlock, int destIndex, int length) {
				Block block = (Block)destBlock;

				int[] arr = GetArray(false);
				int[] dest_arr = block.GetArray(false);

				// Make room in the destination block
				int destb_size = block.Count;
				if (destb_size > 0) {
					Array.Copy(dest_arr, 0, dest_arr, length, destb_size);
				}
				// Copy from this block into the destination block.
				Array.Copy(arr, count - length, dest_arr, 0, length);
				// Alter size of destination and source block.
				block.count += length;
				count -= length;
				// Mark both blocks as changed
				changed = true;
				block.changed = true;
			}

			public void CopyTo(IBlockIndexBlock destBlock) {
				Block block = (Block)destBlock;
				int[] destArr = block.GetArray(false);
				Array.Copy(GetArray(true), 0, destArr, 0, count);
				block.count = count;
				block.changed = true;
			}

			public int CopyTo(int[] destArray, int arrayIndex) {
				Array.Copy(GetArray(true), 0, destArray, arrayIndex, count);
				return count;
			}

			public void Clear() {
				changed = true;
				count = 0;
			}

			public int BinarySearch(object key, IIndexComparer comparer) {
				int[] arr = GetArray(true);
				int low = 0;
				int high = count - 1;

				while (low <= high) {
					int mid = (low + high) / 2;
					int cmp = comparer.Compare(arr[mid], key);

					if (cmp < 0)
						low = mid + 1;
					else if (cmp > 0)
						high = mid - 1;
					else
						return mid; // key found
				}
				return -(low + 1);  // key not found.
			}

			public int SearchFirst(object key, IIndexComparer comparer) {
				int[] arr = GetArray(true);
				int low = 0;
				int high = count - 1;

				while (low <= high) {
					if (high - low <= 2) {
						for (int i = low; i <= high; ++i) {
							int cmp1 = comparer.Compare(arr[i], key);
							if (cmp1 == 0)
								return i;
							if (cmp1 > 0)
								return -(i + 1);
						}
						return -(high + 2);
					}

					int mid = (low + high) / 2;
					int cmp = comparer.Compare(arr[mid], key);

					if (cmp < 0) {
						low = mid + 1;
					} else if (cmp > 0) {
						high = mid - 1;
					} else {
						high = mid;
					}
				}
				return -(low + 1);  // key not found.

			}

			public int SearchLast(object key, IIndexComparer comparer) {
				int[] arr = GetArray(true);
				int low = 0;
				int high = count - 1;

				while (low <= high) {

					if (high - low <= 2) {
						for (int i = high; i >= low; --i) {
							int cmp1 = comparer.Compare(arr[i], key);
							if (cmp1 == 0)
								return i;
							if (cmp1 < 0)
								return -(i + 2);
						}
						return -(low + 1);
					}

					int mid = (low + high) / 2;
					int cmp = comparer.Compare(arr[mid], key);

					if (cmp < 0) {
						low = mid + 1;
					} else if (cmp > 0) {
						high = mid - 1;
					} else {
						low = mid;
					}
				}
				return -(low + 1);  // key not found.
			}

			public int SearchFirst(int value) {
				int[] arr = GetArray(true);
				int low = 0;
				int high = count - 1;

				while (low <= high) {

					if (high - low <= 2) {
						for (int i = low; i <= high; ++i) {
							if (arr[i] == value)
								return i;
							if (arr[i] > value)
								return -(i + 1);
						}
						return -(high + 2);
					}

					int mid = (low + high) / 2;

					if (arr[mid] < value) {
						low = mid + 1;
					} else if (arr[mid] > value) {
						high = mid - 1;
					} else {
						high = mid;
					}
				}
				return -(low + 1);  // key not found.
			}

			public int SearchLast(int value) {
				int[] arr = GetArray(true);
				int low = 0;
				int high = count - 1;

				while (low <= high) {

					if (high - low <= 2) {
						for (int i = high; i >= low; --i) {
							if (arr[i] == value)
								return i;
							if (arr[i] < value)
								return -(i + 2);
						}
						return -(low + 1);
					}

					int mid = (low + high) / 2;

					if (arr[mid] < value) {
						low = mid + 1;
					} else if (arr[mid] > value) {
						high = mid - 1;
					} else {
						low = mid;
					}
				}
				return -(low + 1);  // key not found.
			}

			#region Enumerator

			class Enumerator : IEnumerator<int> {
				private readonly Block block;
				private int index;
				private int[] array;

				public Enumerator(Block block) {
					this.block = block;
					array = block.GetArray(true);
					index = -1;
				}

				public void Dispose() {
				}

				public bool MoveNext() {
					return ++index < array.Length;
				}

				public void Reset() {
					array = block.GetArray(true);
					index = -1;
				}

				public int Current {
					get { return array[index]; }
				}

				object IEnumerator.Current {
					get { return Current; }
				}
			}

			#endregion
		}

		#endregion
	}
}