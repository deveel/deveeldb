// 
//  Copyright 2010-2018 Deveel
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

namespace Deveel.Collections {
	///<summary>
	/// An implementation of <see cref="SortedCollectionBase{TKey,TValue}"/> that stores 
	/// all values in blocks that are entirely stored in main memory.
	///</summary>
	/// <remarks>
	/// This type of structure is useful for large in-memory lists in which a
	/// dd/remove performance must be fast.
	/// </remarks>
	public class SortedCollection<TKey,TValue> : SortedCollectionBase<TKey,TValue> where TValue : IComparable<TValue>, IEquatable<TValue> {
		/// <summary>
		/// Constructs an index with no values.
		///  </summary>
		public SortedCollection() {
		}

		/// <inheritdoc/>
		public SortedCollection(IEnumerable<TValue> values)
			: base(values) {
		}

		/// <inheritdoc/>
		public SortedCollection(ISortedCollection<TKey, TValue> collection)
			: base(collection) {
		}

		/// <inheritdoc/>
		public SortedCollection(IEnumerable<ICollectionBlock<TKey, TValue>> blocks)
			: base(blocks) {
		}

		/// <inheritdoc/>
		protected override ICollectionBlock<TKey, TValue> NewBlock() {
			return new Block(512);
		}

		#region Block

		protected class Block : ICollectionBlock<TKey, TValue> {
			private long count;
			private bool changed;

			protected Block() {
			}

			public Block(int blockSize)
				: this() {
				BaseArray = new BigArray<TValue>(blockSize);
				count = 0;
			}

			protected BigArray<TValue> BaseArray { get; set; }

			protected virtual long ArrayLength {
				get { return BaseArray.Length; }
			}

			public IEnumerator<TValue> GetEnumerator() {
				return new Enumerator(this);
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			ICollectionBlock<TKey, TValue> ICollectionBlock<TKey, TValue>.Next {
				get { return Next; }
				set { Next = (Block) value; }
			}

			private Block Next { get; set; }

			ICollectionBlock<TKey, TValue> ICollectionBlock<TKey, TValue>.Previous {
				get { return Previous; }
				set { Previous = (Block) value; }
			}

			private Block Previous { get; set; }

			/// <inheritdoc/>
			public bool HasChanged {
				get { return changed; }
			}

			/// <inheritdoc/>
			public long Count {
				get { return count; }
				protected set { count = value; }
			}

			/// <inheritdoc/>
			public bool IsFull => count >= ArrayLength;

			/// <inheritdoc/>
			public bool IsEmpty => count <= 0;

			/// <inheritdoc/>
			public virtual TValue Top => GetArray(true)[count - 1];

			/// <inheritdoc/>
			public virtual TValue Bottom {
				get {
					if (count <= 0)
						throw new InvalidOperationException("no bottom value.");

					return GetArray(true)[0];
				}
			}

			/// <inheritdoc/>
			public TValue this[long index] {
				get { return GetArray(true)[index]; }
				set {
					changed = true;
					GetArray(false)[index] = value;
				}
			}

			protected virtual BigArray<TValue> GetArray(bool readOnly) {
				if (readOnly) {
					var newArray = new BigArray<TValue>(BaseArray.Length);
					BaseArray.CopyTo(0, newArray, 0, BaseArray.Length);
					return newArray;
				}
				return BaseArray;
			}

			private static bool IsSmallerOrEqual(TValue x, TValue y) {
				return x.CompareTo(y) <= 0;
			}

			private static bool IsGreaterOrEqual(TValue x, TValue y) {
				return x.CompareTo(y) >= 0;
			}

			private static bool IsGreater(TValue x, TValue y) {
				return x.CompareTo(y) > 0;
			}

			private static bool IsSmaller(TValue x, TValue y) {
				return x.CompareTo(y) < 0;
			}

			/// <inheritdoc/>
			public bool CanContain(long number) {
				return count + number + 1 < ArrayLength;
			}

			/// <inheritdoc/>
			public void Add(TValue value) {
				changed = true;
				var arr = GetArray(false);
				arr[count] = value;
				++count;
			}

			/// <inheritdoc/>
			public TValue RemoveAt(long index) {
				changed = true;
				var arr = GetArray(false);
				var val = arr[index];
				BaseArray.CopyTo(index + 1, arr, index, (count - index));
				--count;
				return val;
			}

			/// <inheritdoc/>
			public long IndexOf(TValue value) {
				var arr = GetArray(true);
				for (var i = count - 1; i >= 0; --i) {
					if (arr[i].Equals(value))
						return i;
				}
				return -1;
			}

			/// <inheritdoc/>
			public long IndexOf(TValue value, long startIndex) {
				var arr = GetArray(true);
				for (var i = startIndex; i < count; ++i) {
					if (arr[i].Equals(value))
						return i;
				}
				return -1;
			}

			/// <inheritdoc/>
			public void Insert(TValue value, long index) {
				changed = true;
				var arr = GetArray(false);
				BaseArray.CopyTo(index, arr, index + 1, (count - index));
				++count;
				arr[index] = value;
			}

			/// <inheritdoc/>
			public void MoveTo(ICollectionBlock<TKey, TValue> destBlock, long destIndex, long length) {
				var block = (Block) destBlock;

				var arr = GetArray(false);
				var destArr = block.GetArray(false);

				// Make room in the destination block
				long destbSize = block.Count;
				if (destbSize > 0) {
					destArr.CopyTo(0, destArr, length, destbSize);
				}

				// Copy from this block into the destination block.
				arr.CopyTo(count - length, destArr, 0, length);
				// Alter size of destination and source block.
				block.count += length;
				count -= length;
				// Mark both blocks as changed
				changed = true;
				block.changed = true;
			}

			/// <inheritdoc/>
			public void CopyTo(ICollectionBlock<TKey, TValue> destBlock) {
				var block = (Block) destBlock;
				var destArr = block.GetArray(false);
				GetArray(true).CopyTo(0, destArr, 0, count);
				block.count = count;
				block.changed = true;
			}

			/// <inheritdoc/>
			public long CopyTo(BigArray<TValue> destArray, long arrayIndex) {
				GetArray(true).CopyTo(0, destArray, arrayIndex, count);
				return count;
			}

			/// <inheritdoc/>
			public void Clear() {
				changed = true;
				count = 0;
			}

			/// <inheritdoc/>
			public long BinarySearch(TKey key, ISortComparer<TKey, TValue> comparer) {
				var arr = GetArray(true);
				long low = 0;
				long high = count - 1;

				while (low <= high) {
					var mid = (low + high)/2;
					int cmp = comparer.Compare(arr[mid], key);

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
			public long SearchFirst(TKey key, ISortComparer<TKey, TValue> comparer) {
				var arr = GetArray(true);
				long low = 0;
				long high = count - 1;

				while (low <= high) {
					if (high - low <= 2) {
						for (var i = low; i <= high; ++i) {
							int cmp1 = comparer.Compare(arr[i], key);
							if (cmp1 == 0)
								return i;

							if (cmp1 > 0)
								return -(i + 1);
						}

						return -(high + 2);
					}

					var mid = (low + high)/2;
					int cmp = comparer.Compare(arr[mid], key);

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
			public long SearchLast(TKey key, ISortComparer<TKey, TValue> comparer) {
				var arr = GetArray(true);
				long low = 0;
				long high = count - 1;

				while (low <= high) {

					if (high - low <= 2) {
						for (var i = high; i >= low; --i) {
							int cmp1 = comparer.Compare(arr[i], key);
							if (cmp1 == 0)
								return i;
							if (cmp1 < 0)
								return -(i + 2);
						}
						return -(low + 1);
					}

					long mid = (low + high)/2;
					int cmp = comparer.Compare(arr[mid], key);

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
			public long SearchFirst(TValue value) {
				var arr = GetArray(true);
				long low = 0;
				long high = count - 1;

				while (low <= high) {

					if (high - low <= 2) {
						for (var i = low; i <= high; ++i) {
							if (arr[i].Equals(value))
								return i;
							if (IsGreater(arr[i], value))
								return -(i + 1);
						}
						return -(high + 2);
					}

					long mid = (low + high)/2;

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
			public long SearchLast(TValue value) {
				var arr = GetArray(true);
				long low = 0;
				long high = count - 1;

				while (low <= high) {

					if (high - low <= 2) {
						for (var i = high; i >= low; --i) {
							if (arr[i].Equals(value))
								return i;
							if (IsSmaller(arr[i], value))
								return -(i + 2);
						}
						return -(low + 1);
					}

					long mid = (low + high)/2;

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

			private class Enumerator : IEnumerator<TValue> {
				private readonly Block block;
				private int index;
				private BigArray<TValue> array;

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

				public TValue Current {
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