//  
//  IIntegerList.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

namespace Deveel.Data.Collections {
	/// <summary>
	/// An interface for querying and accessing a list of primitive integers.
	/// </summary>
	/// <remarks>
	/// The list may or may not be sorted or may be sorted over an 
	/// <see cref="IIndexComparer"/>.
	/// <para>
	/// This interface exposes general list querying/inserting/removing methods.
	/// </para>
	/// <para>
	/// How the list is physically stored is dependant on the implementation of
	/// the interface.
	/// </para>
	/// <para>
	/// An example of an implementation is <see cref="BlockIntegerList"/>.
	/// </para>
	/// </remarks>
	public interface IIntegerList {
		///<summary>
		/// Makes this list immutable effectively making it read-only.
		///</summary>
		/// <remarks>
		/// After this method, any calls to methods that modify the list 
		/// will throw an error.
		/// <para>
		/// Once <see cref="SetImmutable"/> is called, the list can not 
		/// be changed back to being mutable.
		/// </para>
		/// </remarks>
		void SetImmutable();

		/// <summary>
		/// Returns true if this interface is immutable.
		/// </summary>
		bool IsImmutable { get; }

		/// <summary>
		/// The number of integers that are in the list.
		/// </summary>
		int Count { get; }

		/// <summary>
		/// Gets an integer at the given index.
		/// </summary>
		/// <param name="pos">The index of the integer to get.</param>
		/// <returns>
		/// Returns the integer stored at the given <paramref name="pos"/>
		/// within the list.
		/// </returns>
		/// <exception cref="IndexOutOfRangeException">
		/// If the given <paramref name="pos"/> is out of range.
		/// </exception>
		int this[int pos] { get; }

		///<summary>
		/// Adds an integet to the given position in the list.
		///</summary>
		///<param name="val"></param>
		///<param name="pos"></param>
		/// <remarks>
		/// Any values after the given position are shifted forward.
		/// </remarks>
		/// <exception cref="IndexOutOfRangeException">
		/// If the position is out of bounds.
		/// </exception>
		void Add(int val, int pos);

		/// <summary>
		/// Adds a value to the list.
		/// </summary>
		/// <param name="val">The value to add.</param>
		void Add(int val);

		/// <summary>
		/// Removes the value at the given index within the list.
		/// </summary>
		/// <param name="pos">The index within the list from where to remove 
		/// the value.</param>
		/// <returns>
		/// Returns the value that was removed from the given <paramref name="pos"/>.
		/// </returns>
		/// <exception cref="System.IndexOutOfRangeException">
		/// If the given <paramref name="pos"/> is out of range.
		/// </exception>
		int RemoveAt(int pos);

		/// <summary>
		/// Checks if the given value is present within the list.
		/// </summary>
		/// <param name="val">The value to check.</param>
		/// <remarks>
		/// This method assumes the list is sorted.
		/// <para>
		/// If the list is not sorted then this may return <b>false</b>
		/// even if the list does contain the value.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if the given <paramref name="val"/> is found,
		/// otherwise <b>false</b>.
		/// </returns>
		bool Contains(int val);

		/// <summary>
		/// Inserts a value in a sorted order.
		/// </summary>
		/// <param name="val">The value to add.</param>
		void InsertSort(int val);

		/// <summary>
		/// Inserts a value in a sorted order in the list only if not already
		/// present.
		/// </summary>
		/// <param name="val">The value to insert.</param>
		/// <returns>
		/// Returns <b>true</b> if the given <paramref name="val"/> has been
		/// successfully inserted, otherwise (if already present in the list)
		/// returns <b>false</b>.
		/// </returns>
		bool UniqueInsertSort(int val);

		/// <summary>
		/// Removes a value in a sorted order from the list only if already
		/// present.
		/// </summary>
		/// <param name="val">The value to remove.</param>
		/// <returns>
		/// Returns <b>true</b> if the given <paramref name="val"/> has benne 
		/// successfully removed, otherwise (if not already present in the list)
		/// returns <b>false</b>.
		/// </returns>
		/// <exception cref="InvalidOperationException">
		/// If the value removed differs from the given <paramref name="val"/>.
		/// </exception>
		bool RemoveSort(int val);

		// ---------- IIndexComparer methods ----------
		// NOTE: The IIndexComparer methods offer the ability to maintain a set
		//  of index values that reference complex objects.  This is used to manage a
		//  sorted list of integers by their referenced object instead of the int
		//  value itself.  This enables us to create a vaste list of indexes without
		//  having to store the list of objects in memory.

		/// <summary>
		/// Checks if the given key is present within the list.
		/// </summary>
		/// <param name="key">The key to check.</param>
		/// <param name="c"></param>
		/// <remarks>
		/// This method assumes the list is sorted.
		/// If the list is not sorted then this may return <b>false</b>
		/// even if the list does contain the key.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if the given <paramref name="key"/> is found,
		/// otherwise <b>false</b>.
		/// </returns>
		bool Contains(Object key, IIndexComparer c);

		/// <summary>
		/// Inserts the key/value pair into the list at the correct 
		/// sorted position determinated by the given comparer.
		/// </summary>
		/// <param name="key">The key of the value to insert.</param>
		/// <param name="val">The value to insert.</param>
		/// <param name="c">The comparer used to determinate the correct sorted 
		/// order to add the given value.</param>
		/// <remarks>
		/// If the list already contains identical key then the value is add 
		/// to the end of the set of identical values in the list. 
		/// This way, the sort is stable (the order of identical elements does 
		/// not change).
		/// </remarks>
		void InsertSort(Object key, int val, IIndexComparer c);

		/// <summary>
		/// Removes the key/value pair from the list at the correct 
		/// sorted position determinated by the given comparer.
		/// </summary>
		/// <param name="key">The key of the value to remove.</param>
		/// <param name="val">The value to remove.</param>
		/// <param name="c">The comparer used to determinate the correct sorted 
		/// order to remove the given value.</param>
		/// <returns>
		/// Returns the index within the list of the value removed.
		/// </returns>
		int RemoveSort(Object key, int val, IIndexComparer c);

		/// <summary>
		/// Searches the last value for the given key.
		/// </summary>
		/// <param name="key">The key of the value to return.</param>
		/// <param name="c">The comparer used to determinate the
		/// last value in the set to return.</param>
		/// <returns>
		/// Returns the index of the last value in the set for the given
		/// <paramref name="key"/>.
		/// </returns>
		int SearchLast(Object key, IIndexComparer c);

		/// <summary>
		/// Searches the first value for the given key.
		/// </summary>
		/// <param name="key">The key of the value to return.</param>
		/// <param name="c">The comparer used to determinate the
		/// first value in the set to return.</param>
		/// <returns>
		/// Returns the index of the first value in the set for the given
		/// <paramref name="key"/>.
		/// </returns>
		int SearchFirst(Object key, IIndexComparer c);

		// ---------- IIntegerIterator methods ----------

		/// <summary>
		/// Gets a n iterator for the list from a starting offset to an
		/// end offset.
		/// </summary>
		/// <param name="start_offset">The start offset of the iteration
		/// within the list.</param>
		/// <param name="end_offset">The end offset of the iteration
		/// within the list.</param>
		/// <returns>
		/// Returns a <see cref="IIntegerIterator"/> used to iterate through a 
		/// set from the list.
		/// </returns>
		IIntegerIterator GetIterator(int start_offset, int end_offset);

		/// <summary>
		/// Gets an iterator for the list.
		/// </summary>
		/// <returns>
		/// Returns a <see cref="IIntegerIterator"/> used to iterate through a 
		/// set from the list.
		/// </returns>
		IIntegerIterator GetIterator();
	}
}