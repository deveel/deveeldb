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
using System.IO;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Implements a <c>BINARY</c> object that handles a limited number
	/// of bytes, not exceding <see cref="MaxLength"/>.
	/// </summary>
	public struct SqlBinary : ISqlBinary {
		private byte[] bytes;

		/// <summary>
		/// A constant value defining the maximum allowed length of bytes
		/// that this binary can handle.
		/// </summary>
		/// <seealso cref="Length"/>
		public const int MaxLength = Int32.MaxValue - 1;

		public SqlBinary(byte[] source)
			: this(source, source?.Length ?? 0) {
		}

		public SqlBinary(byte[] source, int length)
			: this(source, 0, length) {
		}

		public SqlBinary(byte[] source, int offset, int length) {
			if (source == null)
				throw new ArgumentNullException(nameof(source));

			if (length > MaxLength)
				throw new ArgumentOutOfRangeException(nameof(length));

			bytes = new byte[length];
			Array.Copy(source, offset, bytes, 0, length);
		}

		int IComparable.CompareTo(object obj) {
			return (this as IComparable<ISqlValue>).CompareTo((ISqlValue) obj);
		}

		int IComparable<ISqlValue>.CompareTo(ISqlValue other) {
			throw new NotSupportedException();
		}

		bool ISqlValue.IsComparableTo(ISqlValue other) {
			return false;
		}

		/// <inheritdoc/>
		public IEnumerator<byte> GetEnumerator() {
			if (bytes == null)
				throw new InvalidOperationException("Cannot enumerate a null binary");

			return new ByteEnumerator(this);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public int Length => bytes?.Length ?? 0;

		long ISqlBinary.Length => Length;

		/// <inheritdoc/>
		public Stream GetInput() {
			return new MemoryStream(bytes, false);
		}

		/// <summary>
		/// Returns an array of bytes representing the contents of the binary.
		/// </summary>
		/// <returns></returns>
		public byte[] ToByteArray() {
			if (bytes == null)
				return new byte[0];

			var destArray = new byte[bytes.Length];
			Array.Copy(bytes, 0, destArray, 0, bytes.Length);
			return destArray;
		}

		#region ByteEnumerator

		class ByteEnumerator : IEnumerator<byte> {
			private readonly SqlBinary binary;

			private int index;
			private int count;

			public ByteEnumerator(SqlBinary binary) {
				this.binary = binary;
				Reset();
			}

			public bool MoveNext() {
				return ++index < count;
			}

			public void Reset() {
				index = -1;
				count = binary.bytes.Length;
			}

			public byte Current {
				get {
					AssertCurrent();
					return binary.bytes[index];
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			private void AssertCurrent() {
				if (index<0 || index >= count)
					throw new InvalidOperationException("The enumerator is in an invalid state.");
			}

			public void Dispose() {
			}
		}

		#endregion
	}
}