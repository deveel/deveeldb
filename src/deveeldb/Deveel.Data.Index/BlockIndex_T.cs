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
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Index {
	///<summary>
	/// An implementation of <see cref="BlockIndexBase{T}"/> that stores 
	/// all values in blocks that are entirely stored in main memory.
	///</summary>
	/// <remarks>
	/// This type of structure is useful for large in-memory lists in which a
	/// dd/remove performance must be fast.
	/// </remarks>
	public class BlockIndex<T> : BlockIndexBase<T> where T : IComparable<T>, IEquatable<T> {
		/// <summary>
		/// Constructs an index with no values.
		///  </summary>
		public BlockIndex() {
		}

		/// <inheritdoc/>
		public BlockIndex(IEnumerable<T> values)
			: base(values) {
		}

		/// <inheritdoc/>
		public BlockIndex(IIndex<T> index)
			: base(index) {
		}

		/// <inheritdoc/>
		public BlockIndex(IEnumerable<IIndexBlock<T>> blocks)
			: base(blocks) {
		}

		/// <inheritdoc/>
		protected override IIndexBlock<T> NewBlock() {
			return new Block(512);
		}

		#region Block

		protected class Block : IIndexBlock<T> {
			private int count;
			private bool changed;

			protected Block() {
			}

			public Block(int blockSize)
				: this() {
				BaseArray = new T[blockSize];
				count = 0;
			}

			protected T[] BaseArray { get; set; }

			protected virtual int ArrayLength {
				get { return BaseArray.Length; }
			}

			public IEnumerator<T> GetEnumerator() {
				return new Enumerator(this);
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			IIndexBlock<T> IIndexBlock<T>.Next {
				get { return Next; }
				set { Next = (Block) value; }
			}

			private Block Next { get; set; }

			IIndexBlock<T> IIndexBlock<T>.Previous {
				get { return Previous; }
				set { Previous = (Block) value; }
			}

			private Block Previous { get; set; }

			/// <inheritdoc/>
			public bool HasChanged {
				get { return changed; }
			}

			/// <inheritdoc/>
			public int Count {
				get { return count; }
				protected set { count = value; }
			}

			/// <inheritdoc/>
			public bool IsFull {
				get { return count >= ArrayLength; }
			}

			/// <inheritdoc/>
			public bool IsEmpty {
				get { return count <= 0; }
			}

			/// <inheritdoc/>
			public virtual T Top {
				get { return GetArray(true)[count - 1]; }
			}

			/// <inheritdoc/>
			public virtual T Bottom {
				get {
					if (count <= 0)
						throw new ApplicationException("no bottom value.");

					return GetArray(true)[0];
				}
			}

			/// <inheritdoc/>
			public T this[int index] {
				get { return GetArray(true)[index]; }
				set {
					changed = true;
					GetArray(false)[index] = value;
				}
			}

			protected virtual T[] GetArray(bool readOnly) {
				if (readOnly) {
					var newArray = new T[BaseArray.Length];
					Array.Copy(BaseArray, 0, newArray, 0, BaseArray.Length);
					return newArray;
				}
				return BaseArray;
			}

			private static bool IsSmallerOrEqual(T x, T y) {
				return x.CompareTo(y) <= 0;
			}

			private static bool IsGreaterOrEqual(T x, T y) {
				return x.CompareTo(y) >= 0;
			}

			private static bool IsGreater(T x, T y) {
				return x.CompareTo(y) > 0;
			}

			private static bool IsSmaller(T x, T y) {
				return x.CompareTo(y) < 0;
			}

			/// <inheritdoc/>
			public bool CanContain(int number) {
				return count + number + 1 < ArrayLength;
			}

			/// <inheritdoc/>
			public void Add(T value) {
				changed = true;
				var arr = GetArray(false);
				arr[count] = value;
				++count;
			}

			/// <inheritdoc/>
			public T RemoveAt(int index) {
				changed = true;
				var arr = GetArray(false);
				var val = arr[index];
				Array.Copy(BaseArray, index + 1, arr, index, (count - index));
				--count;
				return val;
			}

			/// <inheritdoc/>
			public int IndexOf(T value) {
				var arr = GetArray(true);
				for (int i = count - 1; i >= 0; --i) {
					if (arr[i].Equals(value))
						return i;
				}
				return -1;
			}

			/// <inheritdoc/>
			public int IndexOf(T value, int startIndex) {
				var arr = GetArray(true);
				for (int i = startIndex; i < count; ++i) {
					if (arr[i].Equals(value))
						return i;
				}
				return -1;
			}

			/// <inheritdoc/>
			public void Insert(T value, int index) {
				changed = true;
				var arr = GetArray(false);
				Array.Copy(BaseArray, index, arr, index + 1, (count - index));
				++count;
				arr[index] = value;
			}

			/// <inheritdoc/>
			public void MoveTo(IIndexBlock<T> destBlock, int destIndex, int length) {
				var block = (Block) destBlock;

				var arr = GetArray(false);
				var destArr = block.GetArray(false);

				// Make room in the destination block
				int destbSize = block.Count;
				if (destbSize > 0) {
					Array.Copy(destArr, 0, destArr, length, destbSize);
				}

				// Copy from this block into the destination block.
				Array.Copy(arr, count - length, destArr, 0, length);
				// Alter size of destination and source block.
				block.count += length;
				count -= length;
				// Mark both blocks as changed
				changed = true;
				block.changed = true;
			}

			/// <inheritdoc/>
			public void CopyTo(IIndexBlock<T> destBlock) {
				var block = (Block) destBlock;
				var destArr = block.GetArray(false);
				Array.Copy(GetArray(true), 0, destArr, 0, count);
				block.count = count;
				block.changed = true;
			}

			/// <inheritdoc/>
			public int CopyTo(T[] destArray, int arrayIndex) {
				Array.Copy(GetArray(true), 0, destArray, arrayIndex, count);
				return count;
			}

			/// <inheritdoc/>
			public void Clear() {
				changed = true;
				count = 0;
			}

			/// <inheritdoc/>
			public int BinarySearch(object key, IIndexComparer<T> comparer) {
				var arr = GetArray(true);
				int low = 0;
				int high = count - 1;

				while (low <= high) {
					int mid = (low + high)/2;
					int cmp = comparer.CompareValue(arr[mid], (DataObject) key);

					if (cmp < 0)
						low = mid + 1;
					else if (cmp > 0)
						high = mid - 1;
					else
						return mid; // key found
				}
				return -(low + 1); // key not found.
			}

			/// <inheritdoc/>
			public int SearchFirst(object key, IIndexComparer<T> comparer) {
				var arr = GetArray(true);
				int low = 0;
				int high = count - 1;

				while (low <= high) {
					if (high - low <= 2) {
						for (int i = low; i <= high; ++i) {
							int cmp1 = comparer.CompareValue(arr[i], (DataObject) key);
							if (cmp1 == 0)
								return i;

							if (cmp1 > 0)
								return -(i + 1);
						}

						return -(high + 2);
					}

					int mid = (low + high)/2;
					int cmp = comparer.CompareValue(arr[mid], (DataObject) key);

					if (cmp < 0) {
						low = mid + 1;
					} else if (cmp > 0) {
						high = mid - 1;
					} else {
						high = mid;
					}
				}
				return -(low + 1); // key not found.

			}

			/// <inheritdoc/>
			public int SearchLast(object key, IIndexComparer<T> comparer) {
				var arr = GetArray(true);
				int low = 0;
				int high = count - 1;

				while (low <= high) {

					if (high - low <= 2) {
						for (int i = high; i >= low; --i) {
							int cmp1 = comparer.CompareValue(arr[i], (DataObject) key);
							if (cmp1 == 0)
								return i;
							if (cmp1 < 0)
								return -(i + 2);
						}
						return -(low + 1);
					}

					int mid = (low + high)/2;
					int cmp = comparer.CompareValue(arr[mid], (DataObject) key);

					if (cmp < 0) {
						low = mid + 1;
					} else if (cmp > 0) {
						high = mid - 1;
					} else {
						low = mid;
					}
				}
				return -(low + 1); // key not found.
			}

			/// <inheritdoc/>
			public int SearchFirst(T value) {
				var arr = GetArray(true);
				int low = 0;
				int high = count - 1;

				while (low <= high) {

					if (high - low <= 2) {
						for (int i = low; i <= high; ++i) {
							if (arr[i].Equals(value))
								return i;
							if (IsGreater(arr[i], value))
								return -(i + 1);
						}
						return -(high + 2);
					}

					int mid = (low + high)/2;

					if (IsSmaller(arr[mid], value)) {
						low = mid + 1;
					} else if (IsGreater(arr[mid], value)) {
						high = mid - 1;
					} else {
						high = mid;
					}
				}
				return -(low + 1); // key not found.
			}

			/// <inheritdoc/>
			public int SearchLast(T value) {
				var arr = GetArray(true);
				int low = 0;
				int high = count - 1;

				while (low <= high) {

					if (high - low <= 2) {
						for (int i = high; i >= low; --i) {
							if (arr[i].Equals(value))
								return i;
							if (IsSmaller(arr[i], value))
								return -(i + 2);
						}
						return -(low + 1);
					}

					int mid = (low + high)/2;

					if (IsSmaller(arr[mid], value)) {
						low = mid + 1;
					} else if (IsGreater(arr[mid], value)) {
						high = mid - 1;
					} else {
						low = mid;
					}
				}
				return -(low + 1); // key not found.
			}

			#region Enumerator

			private class Enumerator : IEnumerator<T> {
				private readonly Block block;
				private int index;
				private T[] array;

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

				public T Current {
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