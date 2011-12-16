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

namespace Deveel.Data {
	/// <summary>
	/// An implementation of an index of values that are stored across an array of blocks.
	/// </summary>
	/// <remarks>
	/// This allows for quicker insertion and deletion of values, including 
	/// other memory saving benefits.
	/// <para>
	/// The class works as follows:
	/// <list type="bullet">
	/// <item>The index can contain any number of <see cref="int">integer</see>
	/// values.</item>
	/// <item>Each value is stored within a block of integers A block is 
	/// of finite size.</item>
	/// <item>When a block becomes full, values are moved around until 
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
	/// <see cref="Item(int)"/>, <see cref="Insert(int, int)"/>, 
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
	public abstract class BlockIndexBase : IIndex {
		private readonly List<IBlockIndexBlock> blocks;

		/// <summary>
		/// The total number of ints in the list.
		/// </summary>
		private int count;

		/// <summary>
		/// If this is set to true, then the index is immutable (we are not permitted
		/// to insert or remove integers from the list).
		/// </summary>
		private bool readOnly;

		protected BlockIndexBase() {
			count = 0;
			readOnly = false;
			blocks = new List<IBlockIndexBlock>(10);
		}

		protected BlockIndexBase(IEnumerable<IBlockIndexBlock> blocks)
			: this() {
			foreach (IBlockIndexBlock block in blocks) {
				this.blocks.Add(block);
				count += block.Count;
			}
		}

		protected BlockIndexBase(IEnumerable<int> values)
			: this() {
			foreach (int value in values) {
				Add(value);
			}
		}

		protected BlockIndexBase(IIndex index)
			: this() {
			if (index is BlockIndexBase) {
				// Optimization for when the input list is a BlockIntegerList
				BlockIndexBase blockIndex = (BlockIndexBase) index;

				List<IBlockIndexBlock> inBlocks = blockIndex.blocks;
				int inBlocksCount = inBlocks.Count;
				// For each block in 'blockIndex'
				for (int i = 0; i < inBlocksCount; ++i) {
					// get the block.
					IBlockIndexBlock block = inBlocks[i];
					// Insert a new block in this object.
					IBlockIndexBlock destBlock = InsertBlock(i, NewBlock());
					// Copy the contents of the source block to the new destination block.
					block.CopyTo(destBlock);
				}

				// Set the size of the list
				count = blockIndex.Count; //count;
			} else {
				// The case when IIntegerList type is not known
				IIndexEnumerator i = index.GetEnumerator();
				while (i.MoveNext()) {
					Add(i.Current);
				}
			}

			// If the given list is immutable then set this list to immutable
			if (index.IsReadOnly)
				readOnly = true;
		}

		protected IList<IBlockIndexBlock> Blocks {
			get { return blocks; }
		}

		public bool IsReadOnly {
			get { return readOnly; }
			set { readOnly = value; }
		}

		public int Count {
			get { return count; }
		}

		public int this[int index] {
			get {
				int size = blocks.Count;
				int start = 0;
				for (int i = 0; i < size; ++i) {
					IBlockIndexBlock block = blocks[i];
					int bsize = block.Count;
					if (index >= start && index < start + bsize)
						return block[index - start];

					start += bsize;
				}

				throw new ArgumentOutOfRangeException("index");
			}
		}

		/// <summary>
		/// Creates a new <see cref="IBlockIndexBlock"/> for the given implementation.
		/// </summary>
		/// <returns></returns>
		protected abstract IBlockIndexBlock NewBlock();

		/// <summary>
		/// Called when the class decides the given <see cref="IBlockIndexBlock"/> 
		/// is no longer needed.
		/// </summary>
		/// <param name="block"></param>
		/// <remarks>
		/// Provided as an event for derived classes to intercept.
		/// </remarks>
		protected virtual void OnDeleteBlock(IBlockIndexBlock block) {
		}


		/// <summary>
		/// Inserts a block at the given position in the index.
		/// </summary>
		/// <param name="index"></param>
		/// <param name="block"></param>
		/// <returns></returns>
		private IBlockIndexBlock InsertBlock(int index, IBlockIndexBlock block) {
			blocks.Insert(index, block);

			// Point to next in the list.
			if (index + 1 < blocks.Count) {
				IBlockIndexBlock nextBlock = blocks[index + 1];
				block.Next = nextBlock;
				nextBlock.Previous = block;
			} else {
				block.Next = null;
			}

			// Point to previous in the list.
			if (index > 0) {
				IBlockIndexBlock prevBlock = blocks[index - 1];
				block.Previous = prevBlock;
				prevBlock.Next = block;
			} else {
				block.Previous = null;
			}

			return block;
		}

		/// <summary>
		/// Removes a block from the given position in the index.
		/// </summary>
		/// <param name="index"></param>
		private void RemoveBlock(int index) {
			// Alter linked list pointers.
			IBlockIndexBlock newPrev = null;
			IBlockIndexBlock newNext = null;
			if (index + 1 < blocks.Count) {
				newNext = blocks[index + 1];
			}
			if (index > 0) {
				newPrev = blocks[index - 1];
			}

			if (newPrev != null) {
				newPrev.Next = newNext;
			}
			if (newNext != null) {
				newNext.Previous = newPrev;
			}

			IBlockIndexBlock beenRemoved = blocks[index];
			blocks.RemoveAt(index);
			OnDeleteBlock(beenRemoved);
		}

		/// <summary>
		/// Inserts a value in the given block position in the list.
		/// </summary>
		/// <param name="value"></param>
		/// <param name="blockIndex"></param>
		/// <param name="block"></param>
		/// <param name="position"></param>
		private void InsertIntoBlock(int value, int blockIndex, IBlockIndexBlock block, int position) {
			block.Insert(position, value);
			++count;
			// Is the block full?
			if (block.IsFull) {
				// We need to move half of the data out of this block into either the
				// next block or create a new block to store it.

				// The size that we going to zap out of this block.
				int moveSize = (block.Count/7) - 1;

				// The block to move half the data from this block.
				IBlockIndexBlock moveTo;

				// Is there a next block?
				if (blockIndex < blocks.Count - 1) {
					IBlockIndexBlock nextBlock = blocks[blockIndex + 1];
					// Yes, can this block contain half the values from this block?
					if (nextBlock.CanContain(moveSize)) {
						moveTo = nextBlock;
					} else {
						// Can't contain so insert a new block.
						moveTo = InsertBlock(blockIndex + 1, NewBlock());
					}

				} else {
					// No next block so create a new block
					moveTo = InsertBlock(blockIndex + 1, NewBlock());
				}

				// 'moveTo' should be set to the block we are to use to move half the
				// data from this block.
				block.MoveTo(moveTo, 0, moveSize);
			}
		}

		/// <summary>
		/// Removes the value from the given position in the specified block.
		/// </summary>
		/// <param name="blockIndex"></param>
		/// <param name="block"></param>
		/// <param name="position"></param>
		/// <remarks>
		/// It returns the value that used to be at that position.
		/// </remarks>
		/// <returns></returns>
		private int RemoveFromBlock(int blockIndex, IBlockIndexBlock block, int position) {
			int old_value = block.RemoveAt(position);
			--count;
			// If we have emptied out this block, then we should remove it from the
			// list.
			if (block.IsEmpty && blocks.Count > 1)
				RemoveBlock(blockIndex);

			return old_value;
		}

		/// <summary>
		/// Uses a binary search algorithm to quickly determine the index of the 
		/// <see cref="IBlockIndexBlock"/> within 'blocks' of the block 
		/// that contains the given key value using the <see cref="IIndexComparer"/> 
		/// as a lookup comparator.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="comparer"></param>
		/// <returns></returns>
		private int FindBlockContaining(object key, IIndexComparer comparer) {
			if (count == 0)
				return -1;

			int low = 0;
			int high = blocks.Count - 1;

			while (low <= high) {
				int mid = (low + high)/2;
				IBlockIndexBlock block = blocks[mid];

				// Is what we are searching for lower than the bottom value?
				if (comparer.Compare(block.Bottom, key) > 0) {
					high = mid - 1;
				}
					// No, then is it greater than the highest value?
				else if (comparer.Compare(block.Top, key) < 0) {
					low = mid + 1;
				}
					// Must be inside this block then!
				else {
					return mid;
				}
			}

			return -(low + 1); // key not found.
		}

		/// <summary>
		/// Uses a binary search algorithm to quickly determine the index of 
		/// the <see cref="IBlockIndexBlock"/> within 'blocks' of the block 
		/// that contains the given key value using the <see cref="IIndexComparer"/> 
		/// as a lookup comparator.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="comparer"></param>
		/// <returns></returns>
		private int FindLastBlock(object key, IIndexComparer comparer) {
			if (count == 0)
				return -1;

			int low = 0;
			int high = blocks.Count - 1;

			while (low <= high) {
				if (high - low <= 2) {
					for (int i = high; i >= low; --i) {
						IBlockIndexBlock block1 = blocks[i];
						if (comparer.Compare(block1.Bottom, key) <= 0) {
							if (comparer.Compare(block1.Top, key) >= 0)
								return i;
							return -(i + 1) - 1;
						}
					}
					return -(low + 1);
				}

				int mid = (low + high)/2;
				IBlockIndexBlock block = blocks[mid];

				// Is what we are searching for lower than the bottom value?
				if (comparer.Compare(block.Bottom, key) > 0) {
					high = mid - 1;
				}
					// No, then is it greater than the highest value?
				else if (comparer.Compare(block.Top, key) < 0) {
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

			return -(low + 1); // key not found.
		}

		/// <summary>
		/// Uses a binary search algorithm to quickly determine the index of the 
		/// <see cref="IBlockIndexBlock"/> within 'blocks' of the block 
		/// that contains the given key value using the <see cref="IIndexComparer"/> 
		/// as a lookup comparator.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="c"></param>
		/// <returns></returns>
		private int FindFirstBlock(object key, IIndexComparer c) {
			if (count == 0) {
				return -1;
			}

			int low = 0;
			int high = blocks.Count - 1;

			while (low <= high) {

				if (high - low <= 2) {
					for (int i = low; i <= high; ++i) {
						IBlockIndexBlock block1 = blocks[i];
						if (c.Compare(block1.Top, key) >= 0) {
							if (c.Compare(block1.Bottom, key) <= 0)
								return i;
							return -(i + 1);
						}
					}
					return -(high + 1) - 1;
				}

				int mid = (low + high)/2;
				IBlockIndexBlock block = blocks[mid];

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

			return -(low + 1); // key not found.
		}

		/// <summary>
		/// Uses a binary search algorithm to quickly determine the index of the 
		/// <see cref="IBlockIndexBlock"/> within 'blocks' of the 
		/// block that contains the given value.
		/// </summary>
		/// <param name="val"></param>
		/// <returns></returns>
		private int FindLastBlock(int val) {
			if (count == 0) {
				return -1;
			}

			int low = 0;
			int high = blocks.Count - 1;

			while (low <= high) {

				if (high - low <= 2) {
					for (int i = high; i >= low; --i) {
						IBlockIndexBlock block1 = blocks[i];
						if (block1.Bottom <= val) {
							if (block1.Top >= val)
								return i;
							return -(i + 1) - 1;
						}
					}
					return -(low + 1);
				}

				int mid = (low + high)/2;
				IBlockIndexBlock block = blocks[mid];

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

			return -(low + 1); // key not found.
		}

		/// <summary>
		/// Checks if the current index is mutable.
		/// </summary>
		/// <remarks>
		/// This is called before any mutable operations on the index: if the index is 
		/// mutable and empty then an empty block is added to the index.
		/// </remarks>
		/// <exception cref="ApplicationException">
		/// Thrown if the index is read-only.
		/// </exception>
		private void CheckImmutable() {
			if (readOnly)
				throw new ApplicationException("Index is read-only.");

			// HACK: We have a side effect of checking whether the list is immutable.
			//   If the block list doesn't contain any entries we add one here.  This
			//   hack reduces the memory requirements.
			if (blocks.Count == 0) {
				InsertBlock(0, NewBlock());
			}
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
			if (array.Length < length || (offset + length) > Count)
				throw new ApplicationException("Size mismatch.");

			foreach (IBlockIndexBlock block in blocks) {
				offset += block.CopyTo(array, offset);
			}
		}

		public void Insert(int index, int value) {
			CheckImmutable();

			int size = blocks.Count;
			int start = 0;
			for (int i = 0; i < size; ++i) {
				IBlockIndexBlock block = blocks[i];
				int bsize = block.Count;
				if (index >= start && index <= start + bsize) {
					InsertIntoBlock(value, i, block, index - start);
					return;
				}
				start += bsize;
			}

			throw new ArgumentOutOfRangeException("index");
		}

		public void Add(int value) {
			CheckImmutable();

			int size = blocks.Count;
			IBlockIndexBlock block = blocks[size - 1];
			InsertIntoBlock(value, size - 1, block, block.Count);
		}

		public int RemoveAt(int index) {
			CheckImmutable();

			int size = blocks.Count;
			int start = 0;
			for (int i = 0; i < size; ++i) {
				IBlockIndexBlock block = blocks[i];
				int bsize = block.Count;
				if (index >= start && index <= start + bsize) {
					return RemoveFromBlock(i, block, index - start);
				}
				start += bsize;
			}
			throw new ArgumentOutOfRangeException("index");
		}

		public bool Contains(int value) {
			int blockIndex = FindLastBlock(value);

			if (blockIndex < 0)
				// We didn't find in the list, so return false.
				return false;

			// We got a block, so find out if it's in the block or not.
			IBlockIndexBlock block = blocks[blockIndex];

			// Find, if not there then return false.
			return block.SearchLast(value) >= 0;
		}

		public void InsertSort(int value) {
			CheckImmutable();

			int blockIndex = FindLastBlock(value);

			if (blockIndex < 0) {
				// Not found a block,
				// The block to insert the value,
				blockIndex = (-(blockIndex + 1)) - 1;
				if (blockIndex < 0) {
					blockIndex = 0;
				}
			}

			// We got a block, so find out if it's in the block or not.
			IBlockIndexBlock block = blocks[blockIndex];

			// The point to insert in the block,
			int i = block.SearchLast(value);
			if (i < 0) {
				i = -(i + 1);
			} else {
				i = i + 1;
				// NOTE: A block can never become totally full so it's always okay to
				//   skip one ahead.
			}

			// Insert value into the block,
			InsertIntoBlock(value, blockIndex, block, i);
		}

		public bool UniqueInsertSort(int value) {
			CheckImmutable();

			int blockIndex = FindLastBlock(value);

			if (blockIndex < 0) {
				// Not found a block,
				// The block to insert the value,
				blockIndex = (-(blockIndex + 1)) - 1;
				if (blockIndex < 0) {
					blockIndex = 0;
				}
			}

			// We got a block, so find out if it's in the block or not.
			IBlockIndexBlock block = blocks[blockIndex];

			// The point to insert in the block,
			int i = block.SearchLast(value);
			if (i < 0) {
				i = -(i + 1);
			} else {
				// This means we found the value in the given block, so return false.
				return false;
			}

			// Insert value into the block,
			InsertIntoBlock(value, blockIndex, block, i);

			// Value inserted so return true.
			return true;
		}

		public bool RemoveSort(int value) {
			CheckImmutable();

			int blockIndex = FindLastBlock(value);

			if (blockIndex < 0) {
				// Not found a block,
				// The block to remove the value,
				blockIndex = (-(blockIndex + 1)) - 1;
				if (blockIndex < 0) {
					blockIndex = 0;
				}
			}

			// We got a block, so find out if it's in the block or not.
			IBlockIndexBlock block = blocks[blockIndex];

			// The point to remove the block,
			int i = block.SearchLast(value);
			if (i < 0) {
				// This means we can't found the value in the given block, so return
				// false.
				return false;
			}

			// Remove value into the block,
			int valRemoved = RemoveFromBlock(blockIndex, block, i);
			if (value != valRemoved)
				throw new ApplicationException("Incorrect value removed.");

			// Value removed so return true.
			return true;
		}

		public bool Contains(object key, IIndexComparer comparer) {
			int blockIndex = FindBlockContaining(key, comparer);

			if (blockIndex < 0)
				// We didn't find in the list, so return false.
				return false;

			// We got a block, so find out if it's in the block or not.
			IBlockIndexBlock block = blocks[blockIndex];

			// Find, if not there then return false.
			return block.BinarySearch(key, comparer) >= 0;
		}

		public void InsertSort(object key, int value, IIndexComparer comparer) {
			CheckImmutable();

			int blockIndex = FindLastBlock(key, comparer);

			if (blockIndex < 0) {
				// Not found a block,
				// The block to insert the value,
				blockIndex = (-(blockIndex + 1)) - 1;
				if (blockIndex < 0) {
					blockIndex = 0;
				}
			}

			// We got a block, so find out if it's in the block or not.
			IBlockIndexBlock block = blocks[blockIndex];

			// The point to insert in the block,
			int i = block.SearchLast(key, comparer);
			if (i < 0) {
				i = -(i + 1);
			} else {
				i = i + 1;
				// NOTE: A block can never become totally full so it's always okay to
				//   skip one ahead.
			}

			// Insert value into the block,
			InsertIntoBlock(value, blockIndex, block, i);
		}

		public int RemoveSort(object key, int value, IIndexComparer comparer) {
			CheckImmutable();

			// Find the range of blocks that the value is in.
			int origBlockIndex = FindFirstBlock(key, comparer);
			int blockIndex = origBlockIndex;
			int lastBlockIndex = blocks.Count - 1;

			if (blockIndex < 0)
				// Not found in a block,
				throw new ApplicationException("Value (" + key + ") was not found in the list.");

			IBlockIndexBlock block = blocks[blockIndex];
			int i = block.IndexOf(value);
			while (i == -1) {
				// If not found, go to next block
				++blockIndex;
				if (blockIndex > lastBlockIndex)
					throw new ApplicationException("Value (" + key + ") was not found in the list.");

				block = blocks[blockIndex];
				// Try and find the value within this block
				i = block.IndexOf(value);
			}

			// Remove value from the block,
			return RemoveFromBlock(blockIndex, block, i);
		}

		public int SearchLast(object key, IIndexComparer comparer) {
			int blockIndex = FindLastBlock(key, comparer);
			int sr;

			if (blockIndex < 0) {
				// Guarenteed not found in any blocks so return start of insert block
				blockIndex = (-(blockIndex + 1)); // - 1;
				sr = -1;
			} else {
				// We got a block, so find out if it's in the block or not.
				IBlockIndexBlock block = blocks[blockIndex];

				// Try and find it in the block,
				sr = block.SearchLast(key, comparer);
			}

			int offset = 0;
			for (int i = 0; i < blockIndex; ++i) {
				IBlockIndexBlock block = blocks[i];
				offset += block.Count;
			}

			return sr >= 0 ? offset + sr : -offset + sr;
		}

		public int SearchFirst(object key, IIndexComparer comparer) {
			int block_num = FindFirstBlock(key, comparer);
			int sr;

			if (block_num < 0) {
				// Guarenteed not found in any blocks so return start of insert block
				block_num = (-(block_num + 1)); // - 1;
				sr = -1;
			} else {
				// We got a block, so find out if it's in the block or not.
				IBlockIndexBlock block = blocks[block_num];

				// Try and find it in the block,
				sr = block.SearchFirst(key, comparer);
			}

			int offset = 0;
			for (int i = 0; i < block_num; ++i) {
				IBlockIndexBlock block = blocks[i];
				offset += block.Count;
			}

			return sr >= 0 ? offset + sr : -offset + sr;
		}

		public IIndexEnumerator GetEnumerator() {
			return GetEnumerator(0, Count - 1);
		}

		public IIndexEnumerator GetEnumerator(int startOffset, int endOffset) {
			return new Enumerator(this, startOffset, endOffset);
		}

		IEnumerator<int> IEnumerable<int>.GetEnumerator() {
			return GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		#region Enumerator

		class Enumerator : IIndexEnumerator {
			private readonly BlockIndexBase index;
			private readonly int startOffset;
			private int endOffset;

			private IBlockIndexBlock currentBlock;
			private int currentBlockSize;
			private int blockIndex;
			private int blockOffset;

			private int currentOffset;

			public Enumerator(BlockIndexBase index, int startOffset, int endOffset) {
				this.index = index;
				this.startOffset = startOffset;
				this.endOffset = endOffset;

				Reset();
			}

			private void SetupVars(int offset) {
				int size = index.blocks.Count;
				int start = 0;
				for (blockIndex = 0; blockIndex < size; ++blockIndex) {
					IBlockIndexBlock block = index.blocks[blockIndex];
					int bsize = block.Count;
					if (offset < start + bsize) {
						blockOffset = offset - start;
						if (blockOffset < 0)
							blockOffset = -1;

						currentBlock = block;
						currentBlockSize = bsize;
						return;
					}
					start += bsize;
				}

				throw new IndexOutOfRangeException("'index' (" + offset + ") out of bounds.");
			}



			public void Dispose() {
			}

			public bool MoveNext() {
				if (currentOffset < endOffset) {
					++currentOffset;

					if (++blockOffset >= currentBlockSize) {
						++blockIndex;
						currentBlock = index.blocks[blockIndex];
						currentBlockSize = currentBlock.Count;
						blockOffset = 0;
					}

					return true;
				}

				return false;
			}

			public void Reset() {
				currentOffset = startOffset - 1;

				if (endOffset >= startOffset) {
					// Setup variables to 1 before the start
					SetupVars(startOffset - 1);
				}
			}

			public int Current {
				get { return currentBlock[blockOffset]; }
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			private void WalkBack() {
				--blockOffset;
				--currentOffset;
				if (blockOffset < 0) {
					if (blockIndex > 0) {
						--blockIndex;
						currentBlock = index.blocks[blockIndex];
						currentBlockSize = currentBlock.Count;
						blockOffset = currentBlock.Count - 1;
					}
				}
			}

			public bool MoveBack() {
				if (currentOffset > startOffset) {
					WalkBack();
					return true;
				}

				return false;
			}

			public void Remove() {
				index.CheckImmutable();

				// NOT ELEGANT: We check 'blocks' size to determine if the value
				//   deletion caused blocks to be removed.  If it did, we set up the
				//   internal variables afresh with a call to 'setupVars'.
				int origBlockCount = index.blocks.Count;
				index.RemoveFromBlock(blockIndex, currentBlock, blockOffset);

				// Did the number of blocks in the list change?
				if (origBlockCount == index.blocks.Count) {
					// HACK: Reduce the current cached block size
					--currentBlockSize;
					WalkBack();
				} else {
					--currentOffset;
					SetupVars(currentOffset);
				}
				--endOffset;
			}
		}

		#endregion
	}
}