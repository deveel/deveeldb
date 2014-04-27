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

using System.Collections.Generic;

namespace Deveel.Data.Index {
	/// <summary>
	/// A block contained in a <see cref="BlockIndex"/>.
	/// </summary>
	/// <remarks>
	/// This exposes the contents of a block of the list.
	/// <para>
	/// An <see cref="BlockIndexBase"/> is a single element of a block of 
	/// integers that makes up some complete list of integers. A block 
	/// encapsulates a set of integers making up the block, and a chain to 
	/// the next and previous block in the hash.
	/// </para>
	/// </remarks>
	public interface IIndexBlock : IEnumerable<int> {
		/// <summary>
		/// Gets or sets the next block in the hash.
		/// </summary>
		IIndexBlock Next { get; set; }

		/// <summary>
		/// Gets or sets the previous block in the hash.
		/// </summary>
		IIndexBlock Previous { get; set; }

		///<summary>
		/// Gets a value indicating if this store has been modified.
		///</summary>
		/// <remarks>
		/// The purpose of this property is to determine if any updates need to be 
		/// made to any persistant representation of this store.
		/// </remarks>
		bool HasChanged { get; }

		///<summary>
		/// Returns the number of entries in this block.
		///</summary>
		int Count { get; }

		/// <summary>
		/// Gets a value indicating if the block is full.
		/// </summary>
		bool IsFull { get; }

		/// <summary>
		/// Gets a value indicating if the block is empty.
		/// </summary>
		bool IsEmpty { get; }

		/// <summary>
		/// Gets the element at the top of the block.
		/// </summary>
		int Top { get; }

		/// <summary>
		/// Gets the element at the bottom of the block.
		/// </summary>
		int Bottom { get; }

		/// <summary>
		/// Gets or sets the element at the given index within the block.
		/// </summary>
		/// <param name="index">The zero-based index, inferior to <see cref="Count"/>,
		/// within the block at which the element is located.</param>
		/// <returns>
		/// Returns a <see cref="int"/> element at the given index within the block.
		/// </returns>
		int this[int index] { get; set; }

		///<summary>
		/// Checks that the block has enough room to fill with the given 
		/// number of integers.
		///</summary>
		///<param name="count">The number of elements willing to allocate into
		/// the block.</param>
		///<returns>
		/// Returns <b>true</b> if the block can contain the number of
		/// entries specified, otherwise <b>false</b>.
		/// </returns>
		bool CanContain(int count);

		/// <summary>
		/// Adss an <see cref="int"/> element to the block.
		/// </summary>
		/// <param name="value">The value to insert into the block.</param>
		void Add(int value);

		/// <summary>
		/// Removes the element at the given index from the block.
		/// </summary>
		/// <param name="index">The index of the element to remove.</param>
		/// <returns>
		/// Returns the element removed from the block.
		/// </returns>
		int RemoveAt(int index);

		///<summary>
		/// Performs an iterative search through the values in the block.
		///</summary>
		///<param name="value"></param>
		///<returns>
		/// Returns the index of the value if found, otherwise it returns -1.
		/// </returns>
		int IndexOf(int value);

		///<summary>
		/// Performs an iterative search from the given position to the end of
		/// the list in the block.
		///</summary>
		///<param name="value"></param>
		///<param name="startIndex"></param>
		///<returns>
		/// Returns the index of the value if found, otherwise it returns -1.
		/// </returns>
		int IndexOf(int value, int startIndex);

		/// <summary>
		/// Inserts an element to the block at the given index.
		/// </summary>
		/// <param name="index"></param>
		/// <param name="value"></param>
		void Insert(int index, int value);

		///<summary>
		/// Moves a set of values from the end of this block and inserts it into the
		/// given block at the destination index specified.
		///</summary>
		///<param name="destBlock">The block where to copy the elements from this block.</param>
		///<param name="destIndex">The index in the destination block from where to start
		/// copying element to.</param>
		///<param name="count">The number of element to copy.</param>
		/// <remarks>
		/// Assumes the destination block has enough room to store the set. Assumes 
		/// <paramref name="destBlock"/> is the same type as this.
		/// </remarks>
		void MoveTo(IIndexBlock destBlock, int destIndex, int count);

		///<summary>
		/// Copies all the data from this block into the given destination block.
		///</summary>
		///<param name="destBlock"></param>
		/// <remarks>
		/// Assumes <paramref name="destBlock"/> is the same class as this.
		/// </remarks>
		void CopyTo(IIndexBlock destBlock);

		///<summary>
		/// Copies all the data from this block into the given array.
		///</summary>
		///<param name="array">The destination array of the copy.</param>
		///<param name="arrayIndex">The index within the destination array
		/// from where to start copying data to.</param>
		///<returns>
		/// Returns the number of elements copied to the array.
		/// </returns>
		int CopyTo(int[] array, int arrayIndex);

		/// <summary>
		/// Clears the block of all elements.
		/// </summary>
		void Clear();

		///<summary>
		/// Performs a binary search of the given value within the block.
		///</summary>
		///<param name="key"></param>
		///<param name="comparer"></param>
		/// <remarks>
		/// Considers each element in the block as a reference to another structure, 
		/// and the block sorted by these structures.
		/// </remarks>
		///<returns></returns>
		int BinarySearch(object key, IIndexComparer comparer);

		///<summary>
		/// Finds the first index in the block that equals the given key.
		///</summary>
		///<param name="key"></param>
		///<param name="comparer"></param>
		/// <remarks>
		/// Considers each element in the block as a reference to another structure, 
		/// and the block sorted by these structures.
		/// </remarks>
		///<returns></returns>
		int SearchFirst(object key, IIndexComparer comparer);

		///<summary>
		/// Finds the last index in the block that equals the given key.
		///</summary>
		///<param name="key"></param>
		///<param name="comparer"></param>
		/// <remarks>
		/// Considers each element in the block as a reference to another structure, 
		/// and the block sorted by these structures.
		/// </remarks>
		///<returns></returns>
		int SearchLast(object key, IIndexComparer comparer);

		///<summary>
		/// Assuming a sorted block, finds the first index in the block that
		/// equals the given value.
		///</summary>
		///<param name="value"></param>
		///<returns></returns>
		int SearchFirst(int value);

		///<summary>
		/// Assuming a sorted block, finds the last index in the block that
		/// equals the given value.
		///</summary>
		///<param name="value"></param>
		///<returns></returns>
		int SearchLast(int value);

	}
}