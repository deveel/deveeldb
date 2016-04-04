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
using System.Threading;
using System.Threading.Tasks;

namespace Deveel.Data.Util {
	public sealed class BigArray<T> : IBigList<T>, IDisposable {
		internal static readonly IEqualityComparer<T> _comparer = EqualityComparer<T>.Default;

		private static readonly T[][] _emptyArray = new T[0][];

		private readonly int _blockLength;
		private readonly T _defaultValue;
		private T[][] _firstArray;
		private int _lastBlockLength;

		public BigArray(long length) 
			: this(length, default(T)) {
		}

		public BigArray(long length, T defaultValue) 
			: this(length, defaultValue, BigArray.DefaultBlockLength) {
		}

		public BigArray(long length, int blockLength) 
			: this(length, default(T), blockLength) {
		}

		public BigArray(long length, T defaultValue, int blockLength) {
			if (blockLength < 1024 * 1024)
				throw new ArgumentOutOfRangeException("blockSize");

			_defaultValue = defaultValue;
			_blockLength = blockLength;
			_length = length;

			if (length == 0) {
				_firstArray = _emptyArray;
				return;
			}

			int numBlocks;
			int lastBlockSize;

			checked {
				numBlocks = (int)(length / blockLength);
				lastBlockSize = (int)(length % blockLength);

				if (lastBlockSize > 0)
					numBlocks++;
				else
					lastBlockSize = blockLength;
			}

			_lastBlockLength = lastBlockSize;
			_firstArray = new T[numBlocks][];
		}

		public void Dispose() {
			_firstArray = null;
		}

		internal long _length;

		public long Length {
			get { return _length; }
		}

		public int BlockLength {
			get { return _blockLength; }
		}

		long IBigList<T>.BigCount {
			get { return Length; }
		}

		void IBigList<T>.Insert(long offset, T item) {
			throw new NotImplementedException();
		}

		void IBigList<T>.RemoveAt(long index) {
			throw new NotImplementedException();
		}

		public T this[long index] {
			get {
				var block = _firstArray[index / _blockLength];
				if (block == null)
					return _defaultValue;

				return block[index % _blockLength];
			}
			set {
				long blockIndex = index / _blockLength;

				var block = _firstArray[blockIndex];
				if (block == null) {
					if (_comparer.Equals(value, _defaultValue))
						return;

					if (blockIndex == _firstArray.Length - 1)
						block = new T[_lastBlockLength];
					else
						block = new T[_blockLength];

					if (!_comparer.Equals(default(T), _defaultValue))
						for (int i = 0; i < block.Length; i++)
							block[i] = _defaultValue;

					_firstArray[blockIndex] = block;
				}

				block[index % _blockLength] = value;
			}
		}

		public void Resize(long newLength) {
			if (newLength < 0)
				throw new ArgumentOutOfRangeException("newLength", "newLength can't be negative.");

			if (newLength == 0) {
				_firstArray = _emptyArray;
				_lastBlockLength = 0;
				_length = 0;
				return;
			}

			int numBlocks;
			int lastBlockSize;
			checked {
				numBlocks = (int)(newLength / _blockLength);
				lastBlockSize = (int)(newLength % _blockLength);

				if (lastBlockSize > 0)
					numBlocks++;
				else
					lastBlockSize = _blockLength;
			}

			_lastBlockLength = lastBlockSize;

			T[][] newArray = _firstArray;
			if (numBlocks != _firstArray.Length) {
				int minBlocks = System.Math.Min(numBlocks, _firstArray.Length);
				newArray = new T[numBlocks][];
				for (int i = 0; i < minBlocks; i++)
					newArray[i] = _firstArray[i];
			}

			var block = newArray[numBlocks - 1];
			if (block != null) {
				int oldLength = block.Length;
				Array.Resize(ref block, _lastBlockLength);
				newArray[numBlocks - 1] = block;

				if (!_comparer.Equals(default(T), _defaultValue))
					for (int i = oldLength; i < _lastBlockLength; i++)
						block[i] = _defaultValue;
			}

			if (_firstArray.Length > 0 && newLength > _length && newArray.Length != _firstArray.Length) {
				int oldLastBlockIndex = _firstArray.Length - 1;
				if (newArray[oldLastBlockIndex] != null)
					Array.Resize(ref newArray[oldLastBlockIndex], _blockLength);
			}

			_firstArray = newArray;
			_length = newLength;
		}

		void ICollection<T>.Add(T item) {
			throw new NotImplementedException();
		}

		void ICollection<T>.Clear() {
			throw new NotImplementedException();
		}

		public bool Contains(T item) {
			return IndexOf(item) != -1;
		}

		void ICollection<T>.CopyTo(T[] array, int arrayIndex) {
			throw new NotImplementedException();
		}

		bool ICollection<T>.Remove(T item) {
			throw new NotSupportedException();
		}

		int ICollection<T>.Count {
			get { return (int) Length; }
		}

		bool ICollection<T>.IsReadOnly {
			get { return false; }
		}

		public bool Contains(T item, long startIndex, long count) {
			return IndexOf(item, startIndex, count) != -1;
		}

		public long IndexOf(T item) {
			return IndexOf(item, 0, _length);
		}

		void IList<T>.Insert(int index, T item) {
			throw new NotImplementedException();
		}

		void IList<T>.RemoveAt(int index) {
			throw new NotImplementedException();
		}

		T IList<T>.this[int index] {
			get { return this[index]; }
			set { this[index] = value; }
		}

		int IList<T>.IndexOf(T item) {
			return (int) IndexOf(item);
		}

		public long IndexOf(T item, long startIndex, long count) {
			if (startIndex < 0 || startIndex > _length)
				throw new ArgumentOutOfRangeException("startIndex");

			if (count == 0)
				return -1;

			long end = startIndex + count;
			if (count < 0 || end > _length)
				throw new ArgumentOutOfRangeException("count");

			long blockIndex = startIndex / _blockLength;
			long positionInBlock = startIndex % _blockLength;
			T[] block = _firstArray[blockIndex];
			long position = startIndex;

			while (true) {
				if (block == null) {
					position += _blockLength - positionInBlock;

					if (position >= end)
						return -1;

					blockIndex++;
					positionInBlock = 0;
					block = _firstArray[blockIndex];
					continue;
				}

				if (_comparer.Equals(block[positionInBlock], item))
					return position;

				position++;

				if (position >= end)
					return -1;

				if (positionInBlock > block.Length) {
					blockIndex++;
					positionInBlock = 0;
					block = _firstArray[blockIndex];
				}
			}
		}

		public long BinarySearch(T item) {
			return _BinarySearch(item, 0, _length, Comparer<T>.Default.Compare);
		}

		public long BinarySearch(T item, Comparison<T> comparer) {
			if (comparer == null)
				comparer = Comparer<T>.Default.Compare;

			return _BinarySearch(item, 0, _length, comparer);
		}

		public long BinarySearch(T item, long startIndex, long count, Comparison<T> comparer = null) {
			if (startIndex < 0 || startIndex > _length)
				throw new ArgumentOutOfRangeException("startIndex");

			if (count == 0)
				return ~startIndex;

			long end = startIndex + count;
			if (count < 0 || end > _length)
				throw new ArgumentOutOfRangeException("count");

			if (comparer == null)
				comparer = Comparer<T>.Default.Compare;

			return _BinarySearch(item, startIndex, count, comparer);
		}

		private long _BinarySearch(T item, long startIndex, long count, Comparison<T> comparer) {
			while (true) {
				if (count == 0)
					return ~startIndex;

				long middle = count / 2;
				long position = startIndex + middle;
				int comparison = comparer(item, this[position]);
				if (comparison == 0)
					return position;

				if (comparison < 0) {
					count = middle;
				} else {
					middle++;
					startIndex += middle;
					count -= middle;
				}
			}
		}

		public void Sort(Comparison<T> comparer = null) {
			if (comparer == null)
				comparer = Comparer<T>.Default.Compare;

			var parallelSort = new BigArrayParallelSort();

			_Sort(parallelSort, 0, _length, comparer);

			parallelSort.Wait();
		}

		public void Sort(long startIndex, long count, Comparison<T> comparer) {
			if (startIndex < 0 || startIndex > _length)
				throw new ArgumentOutOfRangeException("startIndex");

			if (count == 0)
				return;

			long end = startIndex + count;
			if (count < 0 || end > _length)
				throw new ArgumentOutOfRangeException("count");

			if (comparer == null)
				comparer = Comparer<T>.Default.Compare;

			var parallelSort = new BigArrayParallelSort();

			_Sort(parallelSort, startIndex, count, comparer);

			parallelSort.Wait();
		}

		private void _Sort(BigArrayParallelSort parallelSort, long startIndex, long count, Comparison<T> comparer) {
			if (count <= 1)
				return;

			long pivotOffset = _Partition(startIndex, count, comparer);

			// if we don't have more than 10k items, we don't need to try to run in parallel.
			if (parallelSort == null || count < 10000)
				_Sort(parallelSort, startIndex, pivotOffset, comparer);
			else {
				// before putting another task to the threadpool, we verify if the amount of parallel
				// work is not exceeding the number of CPUs.
				// Even if the threadpool can be bigger than the number of CPUs, sorting is a no-wait
				// operation and so putting an extra work to do will only increase the number of task
				// switches.
				int parallelCount = Interlocked.Increment(ref BigArrayParallelSort.ParallelSortCount);
				if (parallelCount >= Environment.ProcessorCount) {
					// we have too many threads in parallel
					// (note that the first thread never stops, that's why I used >= operator).
					Interlocked.Decrement(ref BigArrayParallelSort.ParallelSortCount);

					// do a normal sub-sort.
					_Sort(parallelSort, startIndex, pivotOffset, comparer);
				} else {
					bool shouldProcessNormally = false;

					// ok, we have the right to process in parallel, so let's start by saying we
					// are processing in parallel.
					Interlocked.Increment(ref parallelSort.ExecutingCount);
					try {
						ThreadPool.QueueUserWorkItem
						(
							(x) => {
								// ok, finally we can sort. But, if an exception is thrown, we should redirect it to the
								// main thread.
								try {
									_Sort(parallelSort, startIndex, pivotOffset, comparer);
								} catch (Exception exception) {
									// here we store the exception.
									lock (parallelSort) {
										var exceptions = parallelSort.Exceptions;
										if (exceptions == null) {
											exceptions = new List<Exception>();
											parallelSort.Exceptions = exceptions;
										}

										exceptions.Add(exception);
									}
								} finally {
									// Independent if we had an exception or not, we should decrement
									// both counters.
									Interlocked.Decrement(ref BigArrayParallelSort.ParallelSortCount);

									int parallelRemaining = Interlocked.Decrement(ref parallelSort.ExecutingCount);

									// if we were the last parallel thread, we must notify the main thread if it is waiting
									// for us.
									if (parallelRemaining == 0)
										lock (parallelSort)
											Monitor.Pulse(parallelSort);
								}
							}
						);
					} catch {
						// if an exception was thrown trying to call the thread pool, we simple reduce the
						// count number and do the sort normally.
						// The sort is out of the catch in case an Abort is done.
						Interlocked.Decrement(ref parallelSort.ExecutingCount);
						Interlocked.Decrement(ref BigArrayParallelSort.ParallelSortCount);
						shouldProcessNormally = true;
					}

					if (shouldProcessNormally)
						_Sort(parallelSort, startIndex, pivotOffset, comparer);
				}
			}

			_Sort(parallelSort, startIndex + pivotOffset + 1, count - pivotOffset - 1, comparer);
		}
		private long _Partition(long startIndex, long count, Comparison<T> comparer) {
			long pivotIndex = startIndex + count / 2;
			T pivotValue = this[pivotIndex];

			long right = startIndex + count - 1;
			if (pivotIndex != right)
				this[pivotIndex] = this[right];

			long storeIndex = startIndex;
			for (long index = startIndex; index < right; index++) {
				T valueAtIndex = this[index];
				if (comparer(valueAtIndex, pivotValue) >= 0)
					continue;

				if (index != storeIndex) {
					this[index] = this[storeIndex];
					this[storeIndex] = valueAtIndex;
				}

				storeIndex++;
			}

			if (right != storeIndex)
				this[right] = this[storeIndex];

			this[storeIndex] = pivotValue;

			return storeIndex - startIndex;
		}

		public void Swap(long position1, long position2) {
			if (position1 < 0 || position1 >= _length)
				throw new ArgumentOutOfRangeException("position1");

			if (position2 < 0 || position2 >= _length)
				throw new ArgumentOutOfRangeException("position2");

			if (position1 == position2)
				return;

			T value1 = this[position1];
			this[position1] = this[position2];
			this[position2] = value1;
		}

		public IEnumerator<T> GetEnumerator() {
			int lastBlockIndex = _firstArray.Length - 1;
			for (int i = 0; i <= lastBlockIndex; i++) {
				var block = _firstArray[i];

				if (block == null) {
					int numberOfFakes;
					if (i == lastBlockIndex)
						numberOfFakes = _lastBlockLength;
					else
						numberOfFakes = _blockLength;

					for (int j = 0; j < numberOfFakes; j++)
						yield return _defaultValue;
				} else {
					foreach (var value in block)
						yield return value;
				}
			}
		}

		#region BigArrayParallelSort

		private sealed class BigArrayParallelSort {
			public static int ParallelSortCount;
			public int ExecutingCount;
			public List<Exception> Exceptions;

			public void Wait() {
				lock (this)
					while (ExecutingCount > 0)
						Monitor.Wait(this);

				// TODO: An improvement is to have an aggregate exception like for 4.5
				if (Exceptions != null)
					throw Exceptions[0];
			}
		}

		#endregion

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}
	}
}
