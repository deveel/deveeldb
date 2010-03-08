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

namespace Deveel.Data.Collections {
	/// <summary>
	/// A block contained in a <see cref="IntegerListBlockInterface"/>.
	/// </summary>
	/// <remarks>
	/// This exposes the contents of a block of the list.
	/// <para>
	/// An <see cref="AbstractBlockIntegerList"/> is a single element of a 
	/// block of integers that makes up some complete list of integers. A block 
	/// integer list encapsulates a set of integers making up the block, and a 
	/// chain to the next and previous block in the set.
	/// </para>
	/// </remarks>
	internal abstract class IntegerListBlockInterface {
		/// <summary>
		/// The next block in the chain.
		/// </summary>
		public IntegerListBlockInterface next;

		/// <summary>
		/// The previous block in the chain.
		/// </summary>
		public IntegerListBlockInterface previous;

		/// <summary>
		/// Set to true whenever the integers of this block are changed 
		/// via the mutation methods.
		/// </summary>
		internal bool has_changed;


		///<summary>
		/// Returns true if this store has been modified.
		///</summary>
		/// <remarks>
		/// The purpose of this method is to determine if any updates need to be 
		/// made to any persistant representation of this store.
		/// </remarks>
		public bool HasChanged {
			get { return has_changed; }
		}


		///<summary>
		/// Returns the number of entries in this block.
		///</summary>
		public abstract int Count { get; }

		/// <summary>
		/// Returns true if the block is full.
		/// </summary>
		public abstract bool IsFull { get; }

		/// <summary>
		/// Returns true if the block is empty.
		/// </summary>
		public abstract bool IsEmpty { get; }

		///<summary>
		/// Returns true if the block has enough room to fill with the given 
		/// number of integers.
		///</summary>
		///<param name="number"></param>
		///<returns></returns>
		public abstract bool CanContain(int number);

		/// <summary>
		/// The top int in the list.
		/// </summary>
		public abstract int Top { get; }

		/// <summary>
		/// The bottom int in the list.
		/// </summary>
		public abstract int Bottom { get; }

		/// <summary>
		/// Returns the int at the given position in the array.
		/// </summary>
		/// <param name="pos"></param>
		/// <returns></returns>
		public abstract int this[int pos] { get; }

		/// <summary>
		/// Adds an int to the block.
		/// </summary>
		/// <param name="val"></param>
		public abstract void Add(int val);

		/// <summary>
		/// Removes an Int from the specified position in the block.
		/// </summary>
		/// <param name="pos"></param>
		/// <returns></returns>
		public abstract int RemoveAt(int pos);

		///<summary>
		/// Inserts an int at the given position.
		///</summary>
		///<param name="val"></param>
		///<param name="pos"></param>
		public abstract void InsertAt(int val, int pos);

		///<summary>
		/// Sets an int at the given position, overwriting anything that 
		/// was previously there.
		///</summary>
		///<param name="val"></param>
		///<param name="pos"></param>
		///<returns>
		/// Returns the value that was previously at the element.
		/// </returns>
		public abstract int SetAt(int val, int pos);

		///<summary>
		/// Moves a set of values from the end of this block and inserts it into the
		/// given block at the destination index specified.
		///</summary>
		///<param name="dest_block"></param>
		///<param name="dest_index"></param>
		///<param name="length"></param>
		/// <remarks>
		/// Assumes the destination block has enough room to store the set. Assumes 
		/// <paramref name="dest_block"/> is the same class as this.
		/// </remarks>
		public abstract void MoveTo(IntegerListBlockInterface dest_block, int dest_index, int length);

		///<summary>
		/// Copies all the data from this block into the given destination block.
		///</summary>
		///<param name="dest_block"></param>
		/// <remarks>
		/// Assumes <paramref name="dest_block"/> is the same class as this.
		/// </remarks>
		public abstract void CopyTo(IntegerListBlockInterface dest_block);

		///<summary>
		/// Copies all the data from this block into the given int[] array.
		///</summary>
		///<param name="to"></param>
		///<param name="offset"></param>
		///<returns>
		/// Returns the number of 'int' values copied.
		/// </returns>
		public abstract int CopyTo(int[] to, int offset);

		///<summary>
		/// Clears the object to be re-used.
		///</summary>
		public abstract void Clear();

		///<summary>
		/// Performs an iterative search through the int values in the list.
		///</summary>
		///<param name="val"></param>
		///<returns>
		/// If it's found the index of the value is returned, else it returns -1.
		/// </returns>
		public abstract int IterativeSearch(int val);

		///<summary>
		/// Performs an iterative search from the given position to the end of
		/// the list in the block.
		///</summary>
		///<param name="val"></param>
		///<param name="position"></param>
		///<returns>
		/// If it's found the index of the value is returned, else it returns -1.
		/// </returns>
		public abstract int IterativeSearch(int val, int position);



		// ---------- Sort algorithms ----------

		///<summary>
		/// Considers each int a reference to another structure, and the block
		/// sorted by these structures.
		///</summary>
		///<param name="key"></param>
		///<param name="c"></param>
		/// <remarks>
		/// The method performs a binary search.
		/// </remarks>
		///<returns></returns>
		public abstract int BinarySearch(Object key, IIndexComparer c);

		///<summary>
		/// Considers each int a reference to another structure, and the block
		/// sorted by these structures.
		///</summary>
		///<param name="key"></param>
		///<param name="c"></param>
		/// <remarks>
		/// Finds the first index in the block that equals the given key.
		/// </remarks>
		///<returns></returns>
		public abstract int SearchFirst(Object key, IIndexComparer c);

		///<summary>
		/// Considers each int a reference to another structure, and the block
		/// sorted by these structures.
		///</summary>
		///<param name="key"></param>
		///<param name="c"></param>
		/// <remarks>
		/// Finds the first index in the block that equals the given key.
		/// </remarks>
		///<returns></returns>
		public abstract int SearchLast(Object key, IIndexComparer c);

		///<summary>
		/// Assuming a sorted block, finds the first index in the block that
		/// equals the given value.
		///</summary>
		///<param name="val"></param>
		///<returns></returns>
		public abstract int SearchFirst(int val);

		///<summary>
		/// Assuming a sorted block, finds the first index in the block that
		/// equals the given value.
		///</summary>
		///<param name="val"></param>
		///<returns></returns>
		public abstract int SearchLast(int val);

	}
}