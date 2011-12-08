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
using System.Collections.Generic;
using System.Text;

namespace Deveel.Data.Collections {
	/// <summary>
	/// An implementation of a list of integer values that are stored across
	/// an array of blocks.
	/// </summary>
	/// <remarks>
	/// This allows for quicker insertion and deletion of integer values, including 
	/// other memory saving benefits.
	/// <para>
	/// The class works as follows:
	/// <list type="bullet">
	/// <item>The list can contain any number of <see cref="System.Int32">integer</see>
	/// values.</item>
	/// <item>Each value is stored within a block of integers. A block is 
	/// of finite size.</item>
	/// <item>When a block becomes full, integer values are moved around until 
	/// enough space is free. This may be by inserting a new block or by shifting
	/// informations from one block to another.</item>
	/// <item>When a block becomes empty, it is removed.</item>
	/// </list>
	/// </para>
	/// <para>
	/// The benefits of this system are that inserts and deletes are fast even
	/// for very large lists. There are no megabyte sized arraycopies. Also,
	/// the object could be extended to a version that pages un-used blocks to disk
	/// thus saving precious system memory.
	/// </para>
	/// <para>
	/// <b>Note:</b> The following methods are <b>not</b> optimal:
	/// <see cref="this[int]"/>, <see cref="Add(int, int)"/>, 
	/// <see cref="RemoveAt(int)"/>.<br/>
	/// Specifically, they slow as the specified <i>index</i> nears the end of large lists.
	/// </para>
	/// <para>
	/// This type of structure is very fast for large sorted lists where values can
	/// be inserted at any position within the list. Care needs to be taken for
	/// lists where values are inserted and removed constantly, because
	/// fragmentation of the list blocks can occur. 
	/// For example, adding 60,000 random entries followed by removing 50,000 random 
	/// entries will result in many only partially filled blocks. Since each block 
	/// takes a constant amount of memory, this may not be acceptable.
	/// </para>
	/// </remarks>
	internal abstract class AbstractBlockIntegerList : IIntegerList {
		/// <summary>
		/// The list of blocks (objects in this list are of type
		/// <see cref="IntegerListBlockInterface"/>.
		/// </summary>
		protected ArrayList block_list = new ArrayList(10);

		/// <summary>
		/// The total number of ints in the list.
		/// </summary>
		private int count;

		/// <summary>
		/// If this is set to true, then the list is immutable (we are not permitted
		/// to insert or remove integers from the list).
		/// </summary>
		private bool immutable;


		/// <summary>
		/// Constructs the list.
		/// </summary>
		protected AbstractBlockIntegerList() {
			immutable = false;
			count = 0;
			//    block_size = BLOCK_SIZE;

			//    InsertListBlock(0, NewListBlock());

		}

		/// <summary>
		/// Constructs the list from the given set of initial blocks.
		/// </summary>
		/// <param name="blocks"></param>
		protected AbstractBlockIntegerList(IntegerListBlockInterface[] blocks)
			: this() {
			for (int i = 0; i < blocks.Length; ++i) {
				block_list.Add(blocks[i]);
				count += blocks[i].Count;
			}
		}

		/// <summary>
		/// Constructs the list by copying the contents from an IntegerVector.
		/// </summary>
		/// <param name="ivec"></param>
		protected AbstractBlockIntegerList(IEnumerable<int> ivec)
			: this() {

			foreach (int i in ivec) {
				Add(i);
			}
		}

		/// <summary>
		/// Copies the information from the given BlockIntegerList into a new
		/// object and performs a deep clone of the information in this container.
		/// </summary>
		/// <param name="i_list"></param>
		protected AbstractBlockIntegerList(IIntegerList i_list)
			: this() {

			if (i_list is AbstractBlockIntegerList) {
				// Optimization for when the input list is a BlockIntegerList
				AbstractBlockIntegerList in_list = (AbstractBlockIntegerList)i_list;

				//      block_size = in_list.block_size;

				ArrayList in_blocks = in_list.block_list;
				int in_blocks_count = in_blocks.Count;
				// For each block in 'in_list'
				for (int i = 0; i < in_blocks_count; ++i) {
					// get the block.
					IntegerListBlockInterface block =
						(IntegerListBlockInterface)in_blocks[i];
					// Insert a new block in this object.
					IntegerListBlockInterface dest_block =
						InsertListBlock(i, NewListBlock());
					// Copy the contents of the source block to the new destination block.
					block.CopyTo(dest_block);
				}

				// Set the size of the list
				count = i_list.Count;            //count;

			} else {
				// The case when IIntegerList type is not known
				IIntegerIterator i = i_list.GetIterator();
				while (i.MoveNext()) {
					Add(i.Next);
				}
			}

			// If the given list is immutable then set this list to immutable
			if (i_list.IsImmutable) {
				SetImmutable();
			}

		}



		// ---------- Block operations ----------

		/// <summary>
		/// Creates a new ListBlock for the given implementation.
		/// </summary>
		/// <returns></returns>
		protected abstract IntegerListBlockInterface NewListBlock();

		/// <summary>
		/// Called when the class decides this ListBlock is no longer needed.
		/// </summary>
		/// <param name="list_block"></param>
		/// <remarks>
		/// Provided as an event for derived classes to intercept.
		/// </remarks>
		protected virtual void DeleteListBlock(IntegerListBlockInterface list_block) {
		}

		/// <summary>
		/// Copies the data from each block into the given int[] array.
		/// </summary>
		/// <param name="array"></param>
		/// <param name="offset"></param>
		/// <param name="length"></param>
		/// <remarks>
		/// The int[] array must be big enough to fit all the data in this list.
		/// </remarks>
		internal void CopyToArray(int[] array, int offset, int length) {
			if (array.Length >= length && (offset + length) <= Count) {
				for (int i = 0; i < block_list.Count; ++i) {
					IntegerListBlockInterface block =
						(IntegerListBlockInterface)block_list[i];
					offset += block.CopyTo(array, offset);
				}
				return;
			}
			throw new ApplicationException("Size mismatch.");
		}


		/// <summary>
		/// Inserts a ListBlock at the given block in the list of ListBlock's.
		/// </summary>
		/// <param name="index"></param>
		/// <param name="list_block"></param>
		/// <returns></returns>
		private IntegerListBlockInterface InsertListBlock(int index, IntegerListBlockInterface list_block) {
			block_list.Insert(index, list_block);

			// Point to next in the list.
			if (index + 1 < block_list.Count) {
				IntegerListBlockInterface next_b =
					(IntegerListBlockInterface)block_list[index + 1];
				list_block.next = next_b;
				next_b.previous = list_block;
			} else {
				list_block.next = null;
			}

			// Point to previous in the list.
			if (index > 0) {
				IntegerListBlockInterface previous_b =
					(IntegerListBlockInterface)block_list[index - 1];
				list_block.previous = previous_b;
				previous_b.next = list_block;
			} else {
				list_block.previous = null;
			}

			return list_block;
		}

		/// <summary>
		/// Removes a IntegerListBlockInterface from the given index in the list 
		/// of <see cref="IntegerListBlockInterface"/>'s.
		/// </summary>
		/// <param name="index"></param>
		private void RemoveListBlock(int index) {
			// Alter linked list pointers.
			IntegerListBlockInterface new_prev = null;
			IntegerListBlockInterface new_next = null;
			if (index + 1 < block_list.Count) {
				new_next = (IntegerListBlockInterface)block_list[index + 1];
			}
			if (index > 0) {
				new_prev = (IntegerListBlockInterface)block_list[index - 1];
			}

			if (new_prev != null) {
				new_prev.next = new_next;
			}
			if (new_next != null) {
				new_next.previous = new_prev;
			}

			IntegerListBlockInterface been_removed = (IntegerListBlockInterface)block_list[index];
			block_list.RemoveAt(index);
			DeleteListBlock(been_removed);
		}

		/// <summary>
		/// Inserts a value in the given block position in the list.
		/// </summary>
		/// <param name="val"></param>
		/// <param name="block_index"></param>
		/// <param name="block"></param>
		/// <param name="position"></param>
		private void InsertIntoBlock(int val, int block_index, IntegerListBlockInterface block, int position) {
			block.InsertAt(val, position);
			++count;
			// Is the block full?
			if (block.IsFull) {
				// We need to move half of the data out of this block into either the
				// next block or create a new block to store it.

				// The size that we going to zap out of this block.
				int move_size = (block.Count / 7) - 1;

				// The block to move half the data from this block.
				IntegerListBlockInterface move_to;
				// Is there a next block?
				if (block_index < block_list.Count - 1) {
					IntegerListBlockInterface next_b =
						(IntegerListBlockInterface)block_list[block_index + 1];
					//      IntegerListBlockInterface next_b = block.next;
					//      if (next_b != null) {
					// Yes, can this block contain half the values from this block?
					if (next_b.CanContain(move_size)) {
						move_to = next_b;
					} else {
						// Can't contain so insert a new block.
						move_to = InsertListBlock(block_index + 1, NewListBlock());
					}

				} else {
					// No next block so create a new block
					move_to = InsertListBlock(block_index + 1, NewListBlock());
				}

				// 'move_to' should be set to the block we are to use to move half the
				// data from this block.
				block.MoveTo(move_to, 0, move_size);

			}
		}

		/// <summary>
		/// Removes the value from the given position in the specified block.
		/// </summary>
		/// <param name="block_index"></param>
		/// <param name="block"></param>
		/// <param name="position"></param>
		/// <remarks>
		/// It returns the value that used to be at that position.
		/// </remarks>
		/// <returns></returns>
		protected int RemoveFromBlock(int block_index, IntegerListBlockInterface block, int position) {
			int old_value = block.RemoveAt(position);
			--count;
			// If we have emptied out this block, then we should remove it from the
			// list.
			if (block.IsEmpty && block_list.Count > 1) {
				RemoveListBlock(block_index);
			}

			return old_value;
		}

		/// <summary>
		/// Uses a binary search algorithm to quickly determine the index of the 
		/// <see cref="IntegerListBlockInterface"/> within 'block_list' of the block 
		/// that contains the given key value using the <see cref="IIndexComparer"/> 
		/// as a lookup comparator.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="c"></param>
		/// <returns></returns>
		private int FindBlockContaining(Object key, IIndexComparer c) {
			if (count == 0) {
				return -1;
			}

			int low = 0;
			int high = block_list.Count - 1;

			while (low <= high) {
				int mid = (low + high) / 2;
				IntegerListBlockInterface block =
					(IntegerListBlockInterface)block_list[mid];

				// Is what we are searching for lower than the bottom value?
				if (c.Compare(block.Bottom, key) > 0) {
					high = mid - 1;
				}
					// No, then is it greater than the highest value?
				else if (c.Compare(block.Top, key) < 0) {
					low = mid + 1;
				}
					// Must be inside this block then!
				else {
					return mid;
				}
			}

			//    Console.Out.WriteLine("RETURNING: " + low);

			return -(low + 1);  // key not found.
		}

		/// <summary>
		/// Uses a binary search algorithm to quickly determine the index of 
		/// the <see cref="IntegerListBlockInterface"/> within 'block_list' 
		/// of the block that contains the given key value using the <see cref="IIndexComparer"/> 
		/// as a lookup comparator.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="c"></param>
		/// <returns></returns>
		private int FindLastBlock(Object key, IIndexComparer c) {
			if (count == 0) {
				return -1;
			}

			int low = 0;
			int high = block_list.Count - 1;

			while (low <= high) {

				if (high - low <= 2) {
					for (int i = high; i >= low; --i) {
						IntegerListBlockInterface block1 =
							(IntegerListBlockInterface)block_list[i];
						if (c.Compare(block1.Bottom, key) <= 0) {
							if (c.Compare(block1.Top, key) >= 0) {
								return i;
							} else {
								return -(i + 1) - 1;
							}
						}
					}
					return -(low + 1);
				}

				int mid = (low + high) / 2;
				IntegerListBlockInterface block =
					(IntegerListBlockInterface)block_list[mid];

				// Is what we are searching for lower than the bottom value?
				if (c.Compare(block.Bottom, key) > 0) {
					high = mid - 1;
				}
					// No, then is it greater than the highest value?
				else if (c.Compare(block.Top, key) < 0) {
					low = mid + 1;
				}
					// Equal, so highest must be someplace between mid and high.
				else {
					low = mid;
					if (low == high) {
						return low;
					}
				}
			}

			return -(low + 1);  // key not found.
		}

		/// <summary>
		/// Uses a binary search algorithm to quickly determine the index of the 
		/// <see cref="IntegerListBlockInterface"/> within 'block_list' of the block 
		/// that contains the given key value using the <see cref="IIndexComparer"/> 
		/// as a lookup comparator.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="c"></param>
		/// <returns></returns>
		private int FindFirstBlock(Object key, IIndexComparer c) {
			if (count == 0) {
				return -1;
			}

			int low = 0;
			int high = block_list.Count - 1;

			while (low <= high) {

				if (high - low <= 2) {
					for (int i = low; i <= high; ++i) {
						IntegerListBlockInterface block1 =
							(IntegerListBlockInterface)block_list[i];
						if (c.Compare(block1.Top, key) >= 0) {
							if (c.Compare(block1.Bottom, key) <= 0) {
								return i;
							} else {
								return -(i + 1);
							}
						}
					}
					return -(high + 1) - 1;
				}

				int mid = (low + high) / 2;
				IntegerListBlockInterface block =
					(IntegerListBlockInterface)block_list[mid];

				// Is what we are searching for lower than the bottom value?
				if (c.Compare(block.Bottom, key) > 0) {
					high = mid - 1;
				}
					// No, then is it greater than the highest value?
				else if (c.Compare(block.Top, key) < 0) {
					low = mid + 1;
				}
					// Equal, so highest must be someplace between mid and high.
				else {
					high = mid;
				}
			}

			return -(low + 1);  // key not found.
		}


		/// <summary>
		/// Uses a binary search algorithm to quickly determine the index of the 
		/// <see cref="IntegerListBlockInterface"/> within 'block_list' of the 
		/// block that contains the given value.
		/// </summary>
		/// <param name="val"></param>
		/// <returns></returns>
		private int FindFirstBlock(int val) {
			if (count == 0) {
				return -1;
			}

			int low = 0;
			int high = block_list.Count - 1;

			while (low <= high) {

				if (high - low <= 2) {
					for (int i = low; i <= high; ++i) {
						IntegerListBlockInterface block1 =
							(IntegerListBlockInterface)block_list[i];
						if (block1.Top >= val) {
							if (block1.Bottom <= val) {
								return i;
							} else {
								return -(i + 1);
							}
						}
					}
					return -(high + 1) - 1;
				}

				int mid = (low + high) / 2;
				IntegerListBlockInterface block =
					(IntegerListBlockInterface)block_list[mid];

				// Is what we are searching for lower than the bottom value?
				if (block.Bottom > val) {
					high = mid - 1;
				}
					// No, then is it greater than the highest value?
				else if (block.Top < val) {
					low = mid + 1;
				}
					// Equal, so highest must be someplace between mid and high.
				else {
					high = mid;
				}
			}

			return -(low + 1);  // key not found.
		}

		/// <summary>
		/// Uses a binary search algorithm to quickly determine the index of the 
		/// <see cref="IntegerListBlockInterface"/> within 'block_list' of the 
		/// block that contains the given value.
		/// </summary>
		/// <param name="val"></param>
		/// <returns></returns>
		private int FindLastBlock(int val) {
			if (count == 0) {
				return -1;
			}

			int low = 0;
			int high = block_list.Count - 1;

			while (low <= high) {

				if (high - low <= 2) {
					for (int i = high; i >= low; --i) {
						IntegerListBlockInterface block1 =
							(IntegerListBlockInterface)block_list[i];
						if (block1.Bottom <= val) {
							if (block1.Top >= val) {
								return i;
							} else {
								return -(i + 1) - 1;
							}
						}
					}
					return -(low + 1);
				}

				int mid = (low + high) / 2;
				IntegerListBlockInterface block =
					(IntegerListBlockInterface)block_list[mid];

				// Is what we are searching for lower than the bottom value?
				if (block.Bottom > val) {
					high = mid - 1;
				}
					// No, then is it greater than the highest value?
				else if (block.Top < val) {
					low = mid + 1;
				}
					// Equal, so highest must be someplace between mid and high.
				else {
					low = mid;
					if (low == high) {
						return low;
					}
				}
			}

			return -(low + 1);  // key not found.
		}


		/// <summary>
		/// Checks if the current list is mutable.
		/// </summary>
		/// <remarks>
		/// This is called before any mutable operations on the list.  If the list is 
		/// mutable and empty then an empty block is added to the list.
		/// </remarks>
		/// <exception cref="ApplicationException">
		/// Thrown if the list is immutable.
		/// </exception>
		private void CheckImmutable() {
			if (immutable) {
				throw new ApplicationException("List is immutable.");
			}
				// HACK: We have a side effect of checking whether the list is immutable.
				//   If the block list doesn't contain any entries we add one here.  This
				//   hack reduces the memory requirements.
			else if (block_list.Count == 0) {
				InsertListBlock(0, NewListBlock());
			}
		}

		// ---------- Public methods ----------

		/// <inheritdoc/>
		public void SetImmutable() {
			immutable = true;
		}

		/// <inheritdoc/>
		public bool IsImmutable {
			get { return immutable; }
		}


		// ---------- Standard get/set/remove operations ----------
		//  NOTE: Some of these are not optimal.

		/// <inheritdoc/>
		public int Count {
			get { return count; }
		}

		/// <inheritdoc/>
		public int this[int pos] {
			get {
				int size = block_list.Count;
				int start = 0;
				for (int i = 0; i < size; ++i) {
					IntegerListBlockInterface block =
						(IntegerListBlockInterface) block_list[i];
					int bsize = block.Count;
					if (pos >= start && pos < start + bsize) {
						return block[pos - start];
					}
					start += bsize;
				}
				throw new ApplicationException("'pos' (" + pos + ") out of bounds.");
			}
		}

		/// <inheritdoc/>
		public void Add(int val, int pos) {
			CheckImmutable();

			int size = block_list.Count;
			int start = 0;
			for (int i = 0; i < size; ++i) {
				Object ob = block_list[i];
				IntegerListBlockInterface block = (IntegerListBlockInterface)ob;
				int bsize = block.Count;
				if (pos >= start && pos <= start + bsize) {
					InsertIntoBlock(val, i, block, pos - start);
					return;
				}
				start += bsize;
			}
			throw new ApplicationException("'pos' (" + pos + ") out of bounds.");
		}

		/// <inheritdoc/>
		public void Add(int val) {
			CheckImmutable();

			int size = block_list.Count;
			IntegerListBlockInterface block =
				(IntegerListBlockInterface)block_list[size - 1];
			InsertIntoBlock(val, size - 1, block, block.Count);
		}

		/// <inheritdoc/>
		public int RemoveAt(int pos) {
			CheckImmutable();

			int size = block_list.Count;
			int start = 0;
			for (int i = 0; i < size; ++i) {
				IntegerListBlockInterface block =
					(IntegerListBlockInterface)block_list[i];
				int bsize = block.Count;
				if (pos >= start && pos <= start + bsize) {
					return RemoveFromBlock(i, block, pos - start);
				}
				start += bsize;
			}
			throw new ApplicationException("'pos' (" + pos + ") out of bounds.");
		}

		// ---------- Fast methods ----------

		/// <inheritdoc/>
		public bool Contains(int val) {
			int block_num = FindLastBlock(val);

			if (block_num < 0) {
				// We didn't find in the list, so return false.
				return false;
			}

			// We got a block, so find out if it's in the block or not.
			IntegerListBlockInterface block =
				(IntegerListBlockInterface)block_list[block_num];

			// Find, if not there then return false.
			int sr = block.SearchLast(val);
			return sr >= 0;

		}

		/// <inheritdoc/>
		public void InsertSort(int val) {
			CheckImmutable();

			int block_num = FindLastBlock(val);

			if (block_num < 0) {
				// Not found a block,
				// The block to insert the value,
				block_num = (-(block_num + 1)) - 1;
				if (block_num < 0) {
					block_num = 0;
				}
			}

			// We got a block, so find out if it's in the block or not.
			IntegerListBlockInterface block =
				(IntegerListBlockInterface)block_list[block_num];

			// The point to insert in the block,
			int i = block.SearchLast(val);
			if (i < 0) {
				i = -(i + 1);
			} else {
				i = i + 1;
				// NOTE: A block can never become totally full so it's always okay to
				//   skip one ahead.
			}

			// Insert value into the block,
			InsertIntoBlock(val, block_num, block, i);

		}

		/// <inheritdoc/>
		public bool UniqueInsertSort(int val) {
			CheckImmutable();

			int block_num = FindLastBlock(val);

			if (block_num < 0) {
				// Not found a block,
				// The block to insert the value,
				block_num = (-(block_num + 1)) - 1;
				if (block_num < 0) {
					block_num = 0;
				}
			}

			// We got a block, so find out if it's in the block or not.
			IntegerListBlockInterface block =
				(IntegerListBlockInterface)block_list[block_num];

			// The point to insert in the block,
			int i = block.SearchLast(val);
			if (i < 0) {
				i = -(i + 1);
			} else {
				// This means we found the value in the given block, so return false.
				return false;
			}

			// Insert value into the block,
			InsertIntoBlock(val, block_num, block, i);

			// Value inserted so return true.
			return true;

		}

		/// <inheritdoc/>
		public bool RemoveSort(int val) {
			CheckImmutable();

			int block_num = FindLastBlock(val);

			if (block_num < 0) {
				// Not found a block,
				// The block to remove the value,
				block_num = (-(block_num + 1)) - 1;
				if (block_num < 0) {
					block_num = 0;
				}
			}

			// We got a block, so find out if it's in the block or not.
			IntegerListBlockInterface block =
				(IntegerListBlockInterface)block_list[block_num];

			// The point to remove the block,
			int i = block.SearchLast(val);
			if (i < 0) {
				// This means we can't found the value in the given block, so return
				// false.
				return false;
			}

			// Remove value into the block,
			int val_removed = RemoveFromBlock(block_num, block, i);
			if (val != val_removed) {
				throw new ApplicationException("Incorrect value removed.");
			}

			// Value removed so return true.
			return true;

		}


		/// <inheritdoc/>
		public bool Contains(Object key, IIndexComparer c) {
			int block_num = FindBlockContaining(key, c);

			if (block_num < 0) {
				// We didn't find in the list, so return false.
				return false;
			}

			// We got a block, so find out if it's in the block or not.
			IntegerListBlockInterface block =
				(IntegerListBlockInterface)block_list[block_num];

			// Find, if not there then return false.
			int sr = block.BinarySearch(key, c);
			return sr >= 0;

		}

		/// <inheritdoc/>
		public void InsertSort(Object key, int val, IIndexComparer c) {
			CheckImmutable();

			int block_num = FindLastBlock(key, c);

			if (block_num < 0) {
				// Not found a block,
				// The block to insert the value,
				block_num = (-(block_num + 1)) - 1;
				if (block_num < 0) {
					block_num = 0;
				}
			}

			// We got a block, so find out if it's in the block or not.
			IntegerListBlockInterface block =
				(IntegerListBlockInterface)block_list[block_num];

			// The point to insert in the block,
			int i = block.SearchLast(key, c);
			if (i < 0) {
				i = -(i + 1);
			} else {
				i = i + 1;
				// NOTE: A block can never become totally full so it's always okay to
				//   skip one ahead.
			}

			// Insert value into the block,
			InsertIntoBlock(val, block_num, block, i);

		}

		/// <inheritdoc/>
		public int RemoveSort(Object key, int val, IIndexComparer c) {
			CheckImmutable();

			// Find the range of blocks that the value is in.
			int orig_block_num = FindFirstBlock(key, c);
			int block_num = orig_block_num;
			int l_block_num = block_list.Count - 1;
			//    int l_block_num = FindLastBlock(key, c);

			if (block_num < 0) {
				// Not found in a block,
				throw new ApplicationException("Value (" + key + ") was not found in the list.");
			}

			//    int i = -1;
			IntegerListBlockInterface block =
				(IntegerListBlockInterface)block_list[block_num];
			//    int search_from = block.SearchFirst(key, c);
			int i = block.IterativeSearch(val);
			while (i == -1) {
				// If not found, go to next block
				++block_num;
				if (block_num > l_block_num) {
					throw new ApplicationException("Value (" + key + ") was not found in the list.");
				}
				block = (IntegerListBlockInterface)block_list[block_num];
				// Try and find the value within this block
				i = block.IterativeSearch(val);
			}

			//    int last_block_num = FindLastBlock(key, c);
			//    if (last_block_num > orig_block_num) {
			//      double percent = (double) (block_num - orig_block_num) /
			//                       (double) (last_block_num - orig_block_num);
			//      Console.Out.WriteLine("Block range: " + orig_block_num + " to " +
			//                         last_block_num + " p: " + percent);
			//    }

			// Remove value from the block,
			return RemoveFromBlock(block_num, block, i);

		}

		/// <inheritdoc/>
		public int SearchLast(Object key, IIndexComparer c) {
			int block_num = FindLastBlock(key, c);
			int sr;

			if (block_num < 0) {
				// Guarenteed not found in any blocks so return start of insert block
				block_num = (-(block_num + 1)); // - 1;
				sr = -1;
			} else {
				// We got a block, so find out if it's in the block or not.
				IntegerListBlockInterface block =
					(IntegerListBlockInterface)block_list[block_num];

				// Try and find it in the block,
				sr = block.SearchLast(key, c);
			}

			int offset = 0;
			for (int i = 0; i < block_num; ++i) {
				IntegerListBlockInterface block =
					(IntegerListBlockInterface)block_list[i];
				offset += block.Count;
			}

			if (sr >= 0) {
				return offset + sr;
			} else {
				return -offset + sr;
			}

		}

		/// <inheritdoc/>
		public int SearchFirst(Object key, IIndexComparer c) {
			int block_num = FindFirstBlock(key, c);
			int sr;

			if (block_num < 0) {
				// Guarenteed not found in any blocks so return start of insert block
				block_num = (-(block_num + 1)); // - 1;
				//      Console.Out.WriteLine("BN (" + key + "): " + block_num);
				sr = -1;
			} else {
				// We got a block, so find out if it's in the block or not.
				IntegerListBlockInterface block =
					(IntegerListBlockInterface)block_list[block_num];

				// Try and find it in the block,
				sr = block.SearchFirst(key, c);
			}

			int offset = 0;
			for (int i = 0; i < block_num; ++i) {
				IntegerListBlockInterface block =
					(IntegerListBlockInterface)block_list[i];
				offset += block.Count;
			}

			if (sr >= 0) {
				return offset + sr;
			} else {
				return -offset + sr;
			}

		}

		// ---------- Iterator operations ----------


		/// <summary>
		/// The iterator that walks through the list.
		/// </summary>
		private sealed class BILIterator : IIntegerIterator {

			private AbstractBlockIntegerList bil;
			private int start_offset;
			private int end_offset;
			private IntegerListBlockInterface current_block;
			private int current_block_size;
			private int block_index;
			private int block_offset;
			private int cur_offset;

			public BILIterator(AbstractBlockIntegerList bil, int start_offset, int end_offset) {
				this.bil = bil;
				this.start_offset = start_offset;
				this.end_offset = end_offset;
				cur_offset = start_offset - 1;

				if (end_offset >= start_offset) {
					// Setup variables to 1 before the start
					setupVars(start_offset - 1);
				}

			}

			/**
			 * Sets up the internal variables given an offset.
			 */
			private void setupVars(int pos) {
				int size = bil.block_list.Count;
				int start = 0;
				for (block_index = 0; block_index < size; ++block_index) {
					IntegerListBlockInterface block =
						(IntegerListBlockInterface)bil.block_list[block_index];
					int bsize = block.Count;
					if (pos < start + bsize) {
						block_offset = pos - start;
						if (block_offset < 0) {
							block_offset = -1;
						}
						current_block = block;
						current_block_size = bsize;
						return;
					}
					start += bsize;
				}
				throw new ApplicationException("'pos' (" + pos + ") out of bounds.");
			}


			// ---------- Implemented from IIntegerIterator ----------

			public bool MoveNext() {
				return cur_offset < end_offset;
			}

			public int Next {
				get {
					++block_offset;
					++cur_offset;
					if (block_offset >= current_block_size) {
						++block_index;
						current_block =
							(IntegerListBlockInterface) bil.block_list[block_index];
						//        current_block = current_block.next;
						current_block_size = current_block.Count;
						block_offset = 0;
					}
					return current_block[block_offset];
				}
			}

			public bool MovePrevious() {
				return cur_offset > start_offset;
			}

			private void walkBack() {
				--block_offset;
				--cur_offset;
				if (block_offset < 0) {
					if (block_index > 0) {
						//        if (current_block.previous != null) {
						--block_index;
						current_block =
							(IntegerListBlockInterface)bil.block_list[block_index];
						//          current_block = current_block.previous;
						current_block_size = current_block.Count;
						block_offset = current_block.Count - 1;
					}
				}
			}

			public int Previous {
				get {
					walkBack();
					return current_block[block_offset];
				}
			}

			public void Remove() {
				bil.CheckImmutable();

				// NOT ELEGANT: We check 'block_list' size to determine if the value
				//   deletion caused blocks to be removed.  If it did, we set up the
				//   internal variables afresh with a call to 'setupVars'.
				int orig_block_count = bil.block_list.Count;
				bil.RemoveFromBlock(block_index, current_block, block_offset);

				// Did the number of blocks in the list change?
				if (orig_block_count == bil.block_list.Count) {
					// HACK: Reduce the current cached block size
					--current_block_size;
					walkBack();
				} else {
					--cur_offset;
					setupVars(cur_offset);
				}
				--end_offset;
			}

		}

		/// <inheritdoc/>
		public IIntegerIterator GetIterator(int start_offset, int end_offset) {
			return new BILIterator(this, start_offset, end_offset);
		}

		/// <inheritdoc/>
		public IIntegerIterator GetIterator() {
			return GetIterator(0, Count - 1);
		}




		// ---------- Debugging ----------

		/// <inheritdoc/>
		public override String ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append("Blocks: " + block_list.Count + "\n");
			for (int i = 0; i < block_list.Count; ++i) {
				buf.Append("Block (" + i + "): " + block_list[i].ToString() + "\n");
			}
			return buf.ToString();
		}

		///<summary>
		///</summary>
		///<param name="c"></param>
		public void CheckSorted(IIndexComparer c) {
			IIntegerIterator it = GetIterator(0, Count - 1);
			CheckSorted(it, c);
		}

		///<summary>
		///</summary>
		///<param name="iterator"></param>
		///<param name="c"></param>
		///<exception cref="ApplicationException"></exception>
		public static void CheckSorted(IIntegerIterator iterator, IIndexComparer c) {
			if (iterator.MoveNext()) {
				int last_index = iterator.Next;
				while (iterator.MoveNext()) {
					int cur = iterator.Next;
					if (c.Compare(cur, last_index) < 0) {
						throw new ApplicationException("List not sorted!");
					}
					last_index = cur;
				}
			}
		}
	}
}