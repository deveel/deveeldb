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
using System.Text;

namespace Deveel.Data.Collections {
	///<summary>
	/// An implementation of <see cref="AbstractBlockIntegerList"/> that stores 
	/// all int values in blocks that are entirely stored in main memory.
	///</summary>
	/// <remarks>
	/// This type of structure is useful for large in-memory lists in which a
	/// dd/remove performance must be fast.
	/// </remarks>
	internal class BlockIntegerList : AbstractBlockIntegerList {
		/// <summary>
		/// Constructs the list.
		/// </summary>
		public BlockIntegerList()
			: base() {
		}

		public BlockIntegerList(IntegerVector ivec)
			: base(ivec) {
		}

		/// <summary>
		/// Copies the information from the given BlockIntegerList into a new
		/// object and performs a deep clone of the information in this container.
		/// </summary>
		/// <param name="i_list"></param>
		public BlockIntegerList(IIntegerList i_list)
			: base(i_list) {
		}

		/// <inheritdoc/>
		protected override IntegerListBlockInterface NewListBlock() {
			return new IntArrayListBlock(512);     // (default block size is 512)
		}

		/// <inheritdoc/>
		protected override void DeleteListBlock(IntegerListBlockInterface list_block) {
		}

		// ---------- Inner classes ----------

		///<summary>
		/// The block that contains the actual int values of the list.
		///</summary>
		/// <remarks>
		/// This is made public because it may be useful to derive from this class.
		/// </remarks>
		public class IntArrayListBlock : IntegerListBlockInterface {
			/// <summary>
			/// The array of int's stored in this block.
			/// </summary>
			protected int[] array;

			/// <summary>
			/// The number of block entries in this list.
			/// </summary>
			protected int count;

			/// <summary>
			/// Blank protected constructor.
			/// </summary>
			protected IntArrayListBlock()
				: base() {
			}

			/// <summary>
			/// Constructs the block to a specific size.
			/// </summary>
			/// <param name="block_size"></param>
			public IntArrayListBlock(int block_size)
				: this() {
				array = new int[block_size];
				count = 0;
			}

			/// <summary>
			/// Returns the int array for this block.
			/// </summary>
			/// <param name="immutable">If true then the array object is guarenteed 
			/// to not be mutated.</param>
			/// <returns></returns>
			protected virtual int[] GetArray(bool immutable) {
				return array;
			}

			/// <summary>
			/// Returns the count of int's in this block.
			/// </summary>
			protected virtual int ArrayLength {
				get { return array.Length; }
			}

			/// <inheritdoc/>
			public override int Count {
				get { return count; }
			}

			/// <inheritdoc/>
			public override bool IsFull {
				get { return count >= ArrayLength; }
			}

			/// <inheritdoc/>
			public override bool IsEmpty {
				get { return count <= 0; }
			}

			/// <inheritdoc/>
			public override bool CanContain(int number) {
				return count + number + 1 < ArrayLength;
			}

			/// <inheritdoc/>
			public override int Top {
				get { return GetArray(true)[count - 1]; }
			}

			/// <inheritdoc/>
			public override int Bottom {
				get {
					if (count > 0) {
						return GetArray(true)[0];
					}
					throw new ApplicationException("no bottom integer.");
				}
			}

			/// <inheritdoc/>
			public override int this[int pos] {
				get { return GetArray(true)[pos]; }
			}

			/// <inheritdoc/>
			public override void Add(int val) {
				has_changed = true;
				int[] arr = GetArray(false);
				arr[count] = val;
				++count;
			}

			/// <inheritdoc/>
			public override int RemoveAt(int pos) {
				has_changed = true;
				int[] arr = GetArray(false);
				int val = arr[pos];
				//      Console.Out.WriteLine("[" + (pos + 1) + ", " + pos + ", " + (count - pos) + "]");
				Array.Copy(array, pos + 1, arr, pos, (count - pos));
				--count;
				return val;
			}

			/// <inheritdoc/>
			public override void InsertAt(int val, int pos) {
				has_changed = true;
				int[] arr = GetArray(false);
				Array.Copy(array, pos, arr, pos + 1, (count - pos));
				++count;
				arr[pos] = val;
			}

			/// <inheritdoc/>
			public override int SetAt(int val, int pos) {
				has_changed = true;
				int[] arr = GetArray(false);
				int old = arr[pos];
				arr[pos] = val;
				return old;
			}

			/// <inheritdoc/>
			public override void MoveTo(IntegerListBlockInterface dest_block, int dest_index, int length) {
				IntArrayListBlock block = (IntArrayListBlock)dest_block;

				int[] arr = GetArray(false);
				int[] dest_arr = block.GetArray(false);

				// Make room in the destination block
				int destb_size = block.Count;
				if (destb_size > 0) {
					Array.Copy(dest_arr, 0,
					           dest_arr, length, destb_size);
				}
				// Copy from this block into the destination block.
				Array.Copy(arr, count - length, dest_arr, 0, length);
				// Alter size of destination and source block.
				block.count += length;
				count -= length;
				// Mark both blocks as changed
				has_changed = true;
				block.has_changed = true;
			}

			/// <inheritdoc/>
			public override void CopyTo(IntegerListBlockInterface dest_block) {
				IntArrayListBlock block = (IntArrayListBlock)dest_block;
				int[] dest_arr = block.GetArray(false);
				Array.Copy(GetArray(true), 0, dest_arr, 0, count);
				block.count = count;
				block.has_changed = true;
			}

			/// <inheritdoc/>
			public override int CopyTo(int[] to, int offset) {
				Array.Copy(GetArray(true), 0, to, offset, count);
				return count;
			}

			/// <inheritdoc/>
			public override void Clear() {
				has_changed = true;
				count = 0;
			}

			/// <inheritdoc/>
			public override int IterativeSearch(int val) {
				int[] arr = GetArray(true);
				for (int i = count - 1; i >= 0; --i) {
					if (arr[i] == val) {
						return i;
					}
				}
				return -1;
			}

			/// <inheritdoc/>
			public override int IterativeSearch(int val, int position) {
				int[] arr = GetArray(true);
				for (int i = position; i < count; ++i) {
					if (arr[i] == val) {
						return i;
					}
				}
				return -1;
			}



			// ---------- Sort algorithms ----------

			/// <inheritdoc/>
			public override int BinarySearch(Object key, IIndexComparer c) {
				int[] arr = GetArray(true);
				int low = 0;
				int high = count - 1;

				while (low <= high) {
					int mid = (low + high) / 2;
					int cmp = c.Compare(arr[mid], key);

					if (cmp < 0)
						low = mid + 1;
					else if (cmp > 0)
						high = mid - 1;
					else
						return mid; // key found
				}
				return -(low + 1);  // key not found.
			}


			/// <inheritdoc/>
			public override int SearchFirst(Object key, IIndexComparer c) {
				int[] arr = GetArray(true);
				int low = 0;
				int high = count - 1;

				while (low <= high) {

					if (high - low <= 2) {
						for (int i = low; i <= high; ++i) {
							int cmp1 = c.Compare(arr[i], key);
							if (cmp1 == 0) {
								return i;
							} else if (cmp1 > 0) {
								return -(i + 1);
							}
						}
						return -(high + 2);
					}

					int mid = (low + high) / 2;
					int cmp = c.Compare(arr[mid], key);

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

			/// <inheritdoc/>
			public override int SearchLast(Object key, IIndexComparer c) {
				int[] arr = GetArray(true);
				int low = 0;
				int high = count - 1;

				while (low <= high) {

					if (high - low <= 2) {
						for (int i = high; i >= low; --i) {
							int cmp1 = c.Compare(arr[i], key);
							if (cmp1 == 0) {
								return i;
							} else if (cmp1 < 0) {
								return -(i + 2);
							}
						}
						return -(low + 1);
					}

					int mid = (low + high) / 2;
					int cmp = c.Compare(arr[mid], key);

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

			/// <inheritdoc/>
			public override int SearchFirst(int val) {
				int[] arr = GetArray(true);
				int low = 0;
				int high = count - 1;

				while (low <= high) {

					if (high - low <= 2) {
						for (int i = low; i <= high; ++i) {
							if (arr[i] == val) {
								return i;
							} else if (arr[i] > val) {
								return -(i + 1);
							}
						}
						return -(high + 2);
					}

					int mid = (low + high) / 2;

					if (arr[mid] < val) {
						low = mid + 1;
					} else if (arr[mid] > val) {
						high = mid - 1;
					} else {
						high = mid;
					}
				}
				return -(low + 1);  // key not found.
			}

			/// <inheritdoc/>
			public override int SearchLast(int val) {
				int[] arr = GetArray(true);
				int low = 0;
				int high = count - 1;

				while (low <= high) {

					if (high - low <= 2) {
						for (int i = high; i >= low; --i) {
							if (arr[i] == val) {
								return i;
							} else if (arr[i] < val) {
								return -(i + 2);
							}
						}
						return -(low + 1);
					}

					int mid = (low + high) / 2;

					if (arr[mid] < val) {
						low = mid + 1;
					} else if (arr[mid] > val) {
						high = mid - 1;
					} else {
						low = mid;
					}
				}
				return -(low + 1);  // key not found.
			}

			/// <inheritdoc/>
			public override String ToString() {
				int[] arr = GetArray(true);
				StringBuilder buf = new StringBuilder();
				buf.Append("( VALUES: " + count + " ) ");
				for (int i = 0; i < count; ++i) {
					buf.Append(arr[i]);
					buf.Append(", ");
				}
				return buf.ToString();
			}
		}
	}
}