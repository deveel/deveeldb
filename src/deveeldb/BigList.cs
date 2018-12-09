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

namespace Deveel {
	/// <summary>
	///     Represents a strongly typed list of objects.
	///     Uses BigArray to store objects.
	/// </summary>
	/// <typeparam name="T">Type of elements to store.</typeparam>
	public class BigList<T> : IEnumerable<T> {
		#region Member variables

		/// <summary>
		///     Default capacity, used while adding first element
		///     when capacity is not specified.
		/// </summary>
		private const int DefaultCapacity = 4;

		/// <summary>
		///     Empty array.
		/// </summary>
		private static readonly BigArray<T> _emptyArray = new BigArray<T>(0);

		/// <summary>
		///     BigArray instance to store elements.
		/// </summary>
		private BigArray<T> _items;

		/// <summary>
		///     Holds number of elements present in the BigList.
		/// </summary>
		private long _size;

		#endregion

		#region Constructor

		/// <summary>
		///     Initializes a new instance of the BigList.
		/// </summary>
		public BigList() {
			_items = _emptyArray;
		}

		/// <summary>
		///     Initializes a new instance of the BigList with specified capacity.
		/// </summary>
		/// <param name="capacity">Initial capacity.</param>
		public BigList(long capacity) {
			if (capacity < 0)
				throw new ArgumentOutOfRangeException("capacity");

			_items = new BigArray<T>(capacity);
		}

		/// <summary>
		///     Initializes a new instance of the BigList with elements from the specified collection.
		/// </summary>
		/// <param name="collection">The collection whose elements are copied to the new BigList.</param>
		public BigList(IEnumerable<T> collection) {
			if (collection == null)
				throw new ArgumentNullException("collection");

			var collectionObj = collection as ICollection<T>;
			if (collectionObj != null) {
				var count = collectionObj.Count;
				_items = new BigArray<T>(count);
				var index = 0;
				foreach (var item in collectionObj)
					_items[index++] = item;
				_size = count;
			} else {
				_size = 0;
				_items = new BigArray<T>(DefaultCapacity);
				using (var enumerator = collection.GetEnumerator()) {
					while (enumerator.MoveNext())
						Add(enumerator.Current);
				}
			}
		}

		/// <summary>
		///     Initialize a new big list with a collection and known size.
		/// </summary>
		/// <param name="collection">Items to add</param>
		/// <param name="collectionCount">Size of list</param>
		public BigList(IEnumerable<T> collection, long collectionCount) {
			if (collection == null)
				throw new ArgumentNullException("collection");
			if (collectionCount < 0)
				throw new ArgumentException("Cannot make new big list with < 0 items", "collectionCount");
			_items = new BigArray<T>(collectionCount);
			var index = 0;
			foreach (var item in collection)
				_items[index++] = item;
			_size = collectionCount;
		}

		#endregion

		#region Properties

		/// <summary>
		///     Gets or sets the capacity.
		/// </summary>
		public long Capacity {
			get => _items.Length;
			set {
				if (value < _size)
					throw new ArgumentOutOfRangeException("value");
				if (value != _items.Length)
					if (value > 0)
						if (_size == 0)
							_items = new BigArray<T>(value);
						else
							_items.Resize(value);
					else
						_items = _emptyArray;
			}
		}

		/// <summary>
		///     Gets or sets the number of elements present in the BigList.
		/// </summary>
		public long Count => _size;

		#endregion

		#region Methods

		/// <summary>
		///     Searches for the specified object and returns the zero-based index of the
		///     first occurrence within the entire BigList.
		/// </summary>
		/// <param name="item">
		///     The object to locate in the BigList. The value
		///     can be null for reference types.
		/// </param>
		/// <returns>
		///     The zero-based index of the first occurrence of item within the entire BigList,
		///     if found; otherwise, –1.
		/// </returns>
		public long IndexOf(T item) {
			return _items.IndexOf(item, 0, _size);
		}

		/// <summary>
		///     Searches for the specified object and returns the zero-based index of the
		///     first occurrence within the range of elements in the BigList
		///     that extends from the specified index to the last element.
		/// </summary>
		/// <param name="item">
		///     The object to locate in the BigList. The value
		///     can be null for reference types.
		/// </param>
		/// <param name="startIndex">
		///     The zero-based starting index of the search. 0 (zero)
		///     is valid in an empty BigList.
		/// </param>
		/// <returns>
		///     The zero-based index of the first occurrence of item within the range of
		///     elements in the BigList that extends from index to the last element,
		///     if found; otherwise, –1.
		/// </returns>
		public long IndexOf(T item, long startIndex) {
			return _items.IndexOf(item, startIndex, _size - startIndex);
		}

		/// <summary>
		///     Searches for the specified object and returns the zero-based index of the
		///     first occurrence within the range of elements in the BigList
		///     that starts at the specified index and contains the specified number of elements.
		/// </summary>
		/// <param name="item">
		///     The object to locate in the BigList. The value
		///     can be null for reference types.
		/// </param>
		/// <param name="startIndex">
		///     The zero-based starting index of the search. 0 (zero) is valid in an empty
		///     BigList.
		/// </param>
		/// <param name="count">The number of elements in the section to search.</param>
		/// <returns>
		///     The zero-based index of the first occurrence of item within the range of
		///     elements in the BigList that starts at index and
		///     contains count number of elements, if found; otherwise, –1.
		/// </returns>
		public long IndexOf(T item, long startIndex, long count) {
			if (count > _size - startIndex)
				throw new ArgumentOutOfRangeException("count");

			return _items.IndexOf(item, startIndex, count);
		}

		/// <summary>
		///     Inserts an element into the BigList at the specified index.
		/// </summary>
		/// <param name="index">The zero-based index at which item should be inserted.</param>
		/// <param name="item">The object to insert. The value can be null for reference types.</param>
		public void Insert(long index, T item) {
			if (index > _size)
				throw new ArgumentOutOfRangeException("index");

			if (_size == _items.Length)
				EnsureCapacity(_size + 1);

			if (index < _size)
				for (var i = _size; i > index; i--)
					_items[i] = _items[i - 1];

			_items[index] = item;
			_size++;
		}

		/// <summary>
		///     Removes the element at the specified index of the BigList.
		/// </summary>
		/// <param name="index">The zero-based index of the element to remove.</param>
		public void RemoveAt(long index) {
			if (index >= _size)
				throw new ArgumentOutOfRangeException("index");
			_size--;
			if (index < _size)
				for (var i = index; i < _size; i++)
					_items[i] = _items[i + 1];

			_items[_size] = default(T);
		}

		/// <summary>
		///     Gets or sets the element at the specified index.
		/// </summary>
		/// <param name="index">The zero-based index of the element to get or set.</param>
		/// <returns>The element at the specified index.</returns>
		public T this[long index] {
			get {
				if (index >= _size)
					throw new ArgumentOutOfRangeException("index");
				return _items[index];
			}
			set {
				if (index >= _size)
					throw new ArgumentOutOfRangeException("index");

				_items[index] = value;
			}
		}

		/// <summary>
		///     Adds an object to the end of the BigList.
		/// </summary>
		/// <param name="item">The object to be added to the end of the BigList.</param>
		public void Add(T item) {
			if (_size == _items.Length)
				EnsureCapacity(_size + 1);
			_items[_size++] = item;
		}

		public void AddRange(IEnumerable<T> collection) {
			var collectionObj = collection as ICollection<T>;
			if (collectionObj != null) {
				var count = collectionObj.Count;
				EnsureCapacity(_size + count);

				var index = _size;
				foreach (var item in collectionObj)
					_items[index++] = item;
				_size += count;
			} else if (collection is BigList<T>) {
				var bigList = (BigList<T>) collection;
				var count = bigList.Count;

				EnsureCapacity(_size + count);
				var index = _size;
				foreach (var item in bigList)
					_items[index++] = item;
				_size += count;
			} else if (collection is BigArray<T>) {
				var array = (BigArray<T>) collection;
				var count = array.Length;
				EnsureCapacity(_size + count);
				var index = _size;
				foreach (var item in array)
					_items[index++] = item;

				_size += count;
			} else {
				using (var enumerator = collection.GetEnumerator()) {
					while (enumerator.MoveNext())
						Add(enumerator.Current);
				}
			}
		}

		/// <summary>
		///     Removes all elements from the BigList.
		/// </summary>
		public void Clear() {
			if (_size > 0) {
				_items.Clear();
				_size = 0;
			}
		}

		/// <summary>
		///     Determines whether an element is in the BigList.
		/// </summary>
		/// <param name="item">The object to locate in the BigList.</param>
		/// <returns>true if item is found in the BigList, else false.</returns>
		public bool Contains(T item) {
			return _items.IndexOf(item, 0, _size) >= 0;
		}

		/// <summary>
		///     Copies a range of elements from the BigList to a compatible one-dimensional array.
		/// </summary>
		/// <param name="index">
		///     The zero-based index in the source BigList at
		///     which copying begins.
		/// </param>
		/// <param name="destinationArray">
		///     The one-dimensional array that is the destination of the elements
		///     copied from BigList.
		/// </param>
		/// <param name="count">The number of elements to copy.</param>
		public void CopyTo(long index, T[] destinationArray, long count) {
			CopyTo(index, destinationArray, 0, count);
		}

		/// <summary>
		///     Copies a range of elements from the BigList to a compatible one-dimensional array,
		///     starting at the specified index of the destination array.
		/// </summary>
		/// <param name="index">
		///     The zero-based index in the source BigList at
		///     which copying begins.
		/// </param>
		/// <param name="destinationArray">
		///     The one-dimensional array that is the destination of the elements
		///     copied from BigList.
		/// </param>
		/// <param name="destinationIndex">The zero-based index in destinationArray at which copying begins.</param>
		/// <param name="count">The number of elements to copy.</param>
		public void CopyTo(long index, T[] destinationArray, int destinationIndex, long count) {
			if (count > Count - index)
				throw new ArgumentOutOfRangeException("count");

			_items.CopyTo(index, destinationArray, destinationIndex, count);
		}

		/// <summary>
		///     Removes the first occurrence of a specific object from the BigList.
		/// </summary>
		/// <param name="item">The object to remove from the BigList.</param>
		/// <returns>
		///     true if item is successfully removed; otherwise, false. This method also
		///     returns false if item was not found in the BigList.
		/// </returns>
		public bool Remove(T item) {
			var index = IndexOf(item);
			if (index != -1)
				RemoveAt(index);

			return index != -1;
		}

		/// <summary>
		///     Sets the capacity to the actual number of elements in the BigList,
		///     if that number is less than a threshold value.
		/// </summary>
		public void TrimExcess() {
			var num = (long) (_items.Length * 0.9);
			if (_size < num)
				Capacity = _size;
		}

		/// <summary>
		///     Trims the list and removes all elements above newSize
		/// </summary>
		/// <param name="newSize">size of new array</param>
		public void TrimToSize(long newSize) {
			if (newSize > Count || newSize < 0)
				throw new ArgumentException("Cannot trim BigList class to value less than 0 or larger than original size", nameof(newSize));
			_items.Resize(newSize);
			_size = newSize;
		}

		/// <summary>
		///     Performs the specified action on each element of the BigList.
		/// </summary>
		/// <param name="action">The delegate to perform on each element of the BigList.</param>
		public void ForEach(Action<T> action) {
			if (action == null)
				throw new ArgumentNullException("action");

			for (var i = 0; i < _size; i++)
				action(_items[i]);
		}

		/// <summary>
		///     Returns an enumerator that iterates through the BigList.
		/// </summary>
		public IEnumerator<T> GetEnumerator() {
			for (long i = 0; i < _size; i++)
				yield return this[i];
		}

		/// <summary>
		///     Returns an enumerator that iterates through the BigList.
		/// </summary>
		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public void Sort() {
			BigArraySortUtil<T>.QuickSort(_items, 0, Count - 1);
		}

		// Ensures the capacity.
		private void EnsureCapacity(long minCapacityRequired) {
			if (_items.Length < minCapacityRequired) {
				long newCapacity = 0;
				if (_items.Length == 0) {
					newCapacity = DefaultCapacity;
				} else if (_items.Length * 2 < _items.BlockSize) {
					newCapacity = _items.Length * 2;
				} else {
					var rem = _items.Length % _items.BlockSize;
					if (rem > 0)
						newCapacity = _items.Length + rem;
					else
						newCapacity = _items.Length + _items.BlockSize;
				}

				if (newCapacity < minCapacityRequired)
					newCapacity = minCapacityRequired;

				Capacity = newCapacity;
			}
		}

		#endregion
	}
}