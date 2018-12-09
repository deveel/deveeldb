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
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Deveel {
	public sealed class BigArray<T> : IEnumerable<T> {
		private T[][] items;

		public BigArray(long length) {
			Allocate(length);
		}

		private BigArray(BigArray<T> other) {
			this.items = (T[][]) other.items.Clone();
			BlockSize = other.BlockSize;
			Length = other.Length;
		}

		public long Length { get; private set; }

		public long BlockSize { get; private set; }

		public T this[long index] {
			get => items[(int)(index / BlockSize)][index % BlockSize];

			set => items[(int)(index / BlockSize)][index % BlockSize] = value;
		}

		private void Allocate(long length) {
			if (length < 0)
				throw new ArgumentException("Must specify a length >= 0");

			Length = length;

			if (typeof(T).GetTypeInfo().IsValueType) {
				int itemSize = Marshal.SizeOf<T>();
				BlockSize = (int.MaxValue - 56) / itemSize;
			} else {
				int itemSize = IntPtr.Size;
				BlockSize = ((int.MaxValue - 56) / itemSize) - 1;
			}

			int blockCount = (int)(length / BlockSize);
			if (length > (blockCount * BlockSize))
				blockCount++;

			items = new T[blockCount][];

			for (int i = 0; i < blockCount - 1; i++)
				items[i] = new T[BlockSize];

			if (blockCount > 0) {
				items[blockCount - 1] = new T[length - ((blockCount - 1) * BlockSize)];
			}
		}

		public void Resize(long newSize) {
			if (newSize == Length)
				return;

			int blockCount = (int) (newSize / BlockSize);
			if (newSize > (blockCount * BlockSize))
				blockCount++;

			int previousBlockCount = items.Length;

			int lastBlockSize = (int) (newSize - ((blockCount - 1) * BlockSize));
			int previousLastBlockSize = (int) (Length - ((blockCount - 1) * BlockSize));

			if (previousBlockCount != blockCount) {
				if (previousBlockCount < blockCount) //  Increasing size, make more.
				{
					if (previousLastBlockSize != BlockSize) {
						Array.Resize<T>(ref items[previousBlockCount - 1], (int) BlockSize);
					}

					Array.Resize<T[]>(ref items, blockCount);
					for (int i = previousBlockCount; i < blockCount - 1; i++) {
						items[previousBlockCount] = new T[BlockSize];
					}

					items[blockCount - 1] = new T[lastBlockSize];
				} else // Reducing size - cut off blocks.
				{
					Array.Resize<T[]>(ref items, blockCount);
					Array.Resize<T>(ref items[blockCount - 1], lastBlockSize);
				}
			} else // resize the last block
			{
				Array.Resize<T>(ref items[blockCount - 1], lastBlockSize);
			}

			Length = newSize;
		}

		public void Clear() {
			this.Clear(0, this.Length);
		}

		public void Clear(long startIndex, long count) {
			if ((startIndex < 0) || (startIndex > this.Length)) {
				throw new ArgumentOutOfRangeException(nameof(startIndex));
			}

			if ((count < 0) || (count > (this.Length - startIndex))) {
				throw new ArgumentOutOfRangeException(nameof(count));
			}

			long blockIndex = startIndex / this.BlockSize;
			int start = (int)(startIndex % this.BlockSize);
			count += startIndex;
			for (long i = startIndex; i < count && blockIndex < this.items.Length; blockIndex++) {
				int len = this.items[blockIndex].Length;
				if (i + len > count) {
					len = (int)(count - i);
				}

				Array.Clear(this.items[blockIndex], start, len);
				start = 0;
				i += len;
			}
		}

		public long IndexOf(T item) {
			return this.IndexOf(item, 0, this.Length);
		}

		public long IndexOf(T item, long startIndex) {
			return this.IndexOf(item, startIndex, this.Length - startIndex);
		}

		public long IndexOf(T item, long startIndex, long count) {
			long index = -1;

			if ((startIndex < 0) || (startIndex > this.Length)) {
				throw new ArgumentOutOfRangeException(nameof(startIndex));
			}
			if ((count < 0) || (count > (this.Length - startIndex))) {
				throw new ArgumentOutOfRangeException(nameof(count));
			}

			long blockIndex = startIndex / BlockSize;
			int start = (int)(startIndex % BlockSize);
			count += startIndex;
			for (long i = startIndex; i < count && blockIndex < items.Length; blockIndex++) {
				int len = items[blockIndex].Length;

				if (i + len > count) {
					len = (int)(count - i);
				}

				index = Array.IndexOf<T>(items[blockIndex], item, start, len);
				start = 0;
				if (index != -1) {
					index += (blockIndex * BlockSize);
					break;
				}

				i += len;
			}

			return index;
		}

		public void CopyTo(long index, BigArray<T> destinationArray, long destinationIndex, long count) {
			if (destinationArray == null) {
				throw new ArgumentNullException(nameof(destinationArray));
			}

			if ((index < 0) || (index > this.Length)) {
				throw new ArgumentOutOfRangeException(nameof(index));
			}
			if ((destinationIndex < 0) || (destinationIndex > destinationArray.Length)) {
				throw new ArgumentOutOfRangeException(nameof(destinationIndex));
			}
			if ((count < 0) || (count > (this.Length - index)) || (count > (destinationArray.Length - destinationIndex))) {
				throw new ArgumentOutOfRangeException(nameof(count));
			}

			long destIndex = destinationIndex;
			for (long i = index; i < index + count; i++)
				destinationArray[destIndex++] = this[i];
		}

		public void CopyTo(long index, T[] destinationArray, long count) {
			this.CopyTo(index, destinationArray, 0, count);
		}

		public void CopyTo(long index, T[] destinationArray, int destinationIndex, long count) {
			if (destinationArray == null) {
				throw new ArgumentNullException(nameof(destinationArray));
			}
			if ((index < 0) || (index > this.Length)) {
				throw new ArgumentOutOfRangeException(nameof(index));
			}
			if ((destinationIndex < 0) || (destinationIndex > destinationArray.Length)) {
				throw new ArgumentOutOfRangeException(nameof(destinationIndex));
			}
			if ((count < 0) || (count > (this.Length - index)) || (count > (destinationArray.Length - destinationIndex))) {
				throw new ArgumentOutOfRangeException(nameof(count));
			}

			int destIndex = destinationIndex;
			for (long i = index; i < index + count; i++)
				destinationArray[destIndex++] = this[i];
		}

		public IEnumerator<T> GetEnumerator() {
			return items.SelectMany(x => x).GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public static void Clear(BigArray<T> array, long startIndex, long count) {
			if ((startIndex < 0) || (startIndex > array.Length)) {
				throw new ArgumentOutOfRangeException(nameof(startIndex));
			}

			if ((count < 0) || (count > (array.Length - startIndex))) {
				throw new ArgumentOutOfRangeException(nameof(count));
			}

			long blockIndex = startIndex / array.BlockSize;
			int start = (int)(startIndex % array.BlockSize);
			count += startIndex;
			for (long i = startIndex; i < count && blockIndex < array.items.Length; blockIndex++) {
				int len = array.items[blockIndex].Length;
				if (i + len > count) {
					len = (int)(count - i);
				}

				Array.Clear(array.items[blockIndex], start, len);
				start = 0;
				i += len;
			}
		}

		public static void QuickSort(BigArray<T> array, long offset, long count) {
			BigArraySortUtil<T>.QuickSort(array, offset, count);
		}

		public static void QuickSort(BigArray<T> array) {
			QuickSort(array, 0, array.Length - 1);
		}

		public BigArray<T> Clone() {
			return new BigArray<T>(this);
		}
	}
}