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

using System.Collections.Generic;

namespace Deveel.Collections {
	/// <summary>
	/// A block contained in a <see cref="SortedCollection{TKey,TValue}"/>.
	/// </summary>
	/// <remarks>
	/// This exposes the contents of a block of the list.
	/// <para>
	/// An <see cref="SortedCollectionBase{TKey,TValue}"/> is a single element of a block of 
	/// integers that makes up some complete list of integers. A block 
	/// encapsulates a set of integers making up the block, and a chain to 
	/// the next and previous block in the hash.
	/// </para>
	/// </remarks>
	/// <typeparam name="T"></typeparam>
	public interface ICollectionBlock<TKey, TValue> : IEnumerable<TValue> {
		/// <summary>
		/// Gets or sets the next block in the hash.
		/// </summary>
		ICollectionBlock<TKey, TValue> Next { get; set; }

		/// <summary>
		/// Gets or sets the previous block in the hash.
		/// </summary>
		ICollectionBlock<TKey, TValue> Previous { get; set; }

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
		long Count { get; }

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
		TValue Top { get; }

		/// <summary>
		/// Gets the element at the bottom of the block.
		/// </summary>
		TValue Bottom { get; }

		/// <summary>
		/// Gets or sets the element at the given collection within the block.
		/// </summary>
		/// <param name="index">The zero-based collection, inferior to <see cref="Count"/>,
		/// within the block at which the element is located.</param>
		/// <returns>
		/// Returns a <see cref="int"/> element at the given collection within the block.
		/// </returns>
		TValue this[long index] { get; set; }

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
		bool CanContain(long count);

		/// <summary>
		/// Adss an element to the block.
		/// </summary>
		/// <param name="value">The value to insert into the block.</param>
		void Add(TValue value);

		/// <summary>
		/// Removes the element at the given collection from the block.
		/// </summary>
		/// <param name="index">The collection of the element to remove.</param>
		/// <returns>
		/// Returns the element removed from the block.
		/// </returns>
		TValue RemoveAt(long index);

		///<summary>
		/// Performs an iterative search through the values in the block.
		///</summary>
		///<param name="value"></param>
		///<returns>
		/// Returns the collection of the value if found, otherwise it returns -1.
		/// </returns>
		long IndexOf(TValue value);

		///<summary>
		/// Performs an iterative search from the given position to the end of
		/// the list in the block.
		///</summary>
		///<param name="value"></param>
		///<param name="startIndex"></param>
		///<returns>
		/// Returns the collection of the value if found, otherwise it returns -1.
		/// </returns>
		long IndexOf(TValue value, long startIndex);

		/// <summary>
		/// Inserts an element to the block at the given collection.
		/// </summary>
		/// <param name="index"></param>
		/// <param name="value"></param>
		void Insert(TValue value, long index);

		///<summary>
		/// Moves a set of values from the end of this block and inserts it into the
		/// given block at the destination collection specified.
		///</summary>
		///<param name="destBlock">The block where to copy the elements from this block.</param>
		///<param name="destIndex">The collection in the destination block from where to start
		/// copying element to.</param>
		///<param name="count">The number of element to copy.</param>
		/// <remarks>
		/// Assumes the destination block has enough room to store the set. Assumes 
		/// <paramref name="destBlock"/> is the same type as this.
		/// </remarks>
		void MoveTo(ICollectionBlock<TKey, TValue> destBlock, long destIndex, long count);

		///<summary>
		/// Copies all the data from this block into the given destination block.
		///</summary>
		///<param name="destBlock"></param>
		/// <remarks>
		/// Assumes <paramref name="destBlock"/> is the same class as this.
		/// </remarks>
		void CopyTo(ICollectionBlock<TKey, TValue> destBlock);

		///<summary>
		/// Copies all the data from this block into the given array.
		///</summary>
		///<param name="array">The destination array of the copy.</param>
		///<param name="arrayIndex">The collection within the destination array
		/// from where to start copying data to.</param>
		///<returns>
		/// Returns the number of elements copied to the array.
		/// </returns>
		long CopyTo(BigArray<TValue> array, long arrayIndex);

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
		long BinarySearch(TKey key, ISortComparer<TKey, TValue> comparer);

		///<summary>
		/// Finds the first collection in the block that equals the given key.
		///</summary>
		///<param name="key"></param>
		///<param name="comparer"></param>
		/// <remarks>
		/// Considers each element in the block as a reference to another structure, 
		/// and the block sorted by these structures.
		/// </remarks>
		///<returns></returns>
		long SearchFirst(TKey key, ISortComparer<TKey, TValue> comparer);

		///<summary>
		/// Finds the last collection in the block that equals the given key.
		///</summary>
		///<param name="key"></param>
		///<param name="comparer"></param>
		/// <remarks>
		/// Considers each element in the block as a reference to another structure, 
		/// and the block sorted by these structures.
		/// </remarks>
		///<returns></returns>
		long SearchLast(TKey key, ISortComparer<TKey, TValue> comparer);

		///<summary>
		/// Assuming a sorted block, finds the first collection in the block that
		/// equals the given value.
		///</summary>
		///<param name="value"></param>
		///<returns></returns>
		long SearchFirst(TValue value);

		///<summary>
		/// Assuming a sorted block, finds the last collection in the block that
		/// equals the given value.
		///</summary>
		///<param name="value"></param>
		///<returns></returns>
		long SearchLast(TValue value);
	}
}