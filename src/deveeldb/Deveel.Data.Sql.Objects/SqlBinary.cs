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
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Deveel.Data.Sql.Objects {
	/// <summary>
	/// Implements a <c>BINARY</c> object that handles a limited number
	/// of bytes, not exceding <see cref="MaxLength"/>.
	/// </summary>
	[Serializable]
	public struct SqlBinary : ISqlBinary {
		private readonly byte[] bytes;

		/// <summary>
		/// A constant value defining the maximum allowed length of bytes
		/// that this binary can handle.
		/// </summary>
		/// <seealso cref="Length"/>
		public const int MaxLength = Int16.MaxValue;

		public SqlBinary(byte[] source)
			: this(source, source == null ? 0 : source.Length) {
		}

		public SqlBinary(byte[] source, int length)
			: this(source, 0, length) {
		}

		public SqlBinary(byte[] source, int offset, int length)
			: this() {
			if (source == null) {
				bytes = null;
			} else {
				if (length > MaxLength)
					throw new ArgumentOutOfRangeException("length");
				
				bytes = new byte[length];
				Array.Copy(source, offset, bytes, 0, length);
			}
		}

		int IComparable.CompareTo(object obj) {
			return (this as IComparable<ISqlObject>).CompareTo((ISqlObject) obj);
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			throw new NotSupportedException();
		}

		/// <inheritdoc/>
		public bool IsNull {
			get { return bytes == null; }
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return false;
		}

		/// <inheritdoc/>
		public IEnumerator<byte> GetEnumerator() {
			throw new NotImplementedException();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		/// <inheritdoc/>
		public long Length {
			get { return bytes == null ? 0L : bytes.LongLength; }
		}

		/// <inheritdoc/>
		public Stream GetInput() {
			if (IsNull)
				return Stream.Null;

			return new MemoryStream(bytes, false);
		}

		/// <summary>
		/// Materializes the contents of the binary into a finite object.
		/// </summary>
		/// <returns>
		/// Returns an <seealso cref="object"/> that is the result of
		/// the deserialization of the binary data handled by this object.
		/// </returns>
		public object ToObject() {
			using (var input = GetInput()) {
				var fornatter = new BinaryFormatter();
				return fornatter.Deserialize(input);
			}
		}

		/// <summary>
		/// Creates an instance of <seealso cref="SqlBinary"/> from the
		/// dematerialization of the given object into its binary
		/// equivalent.
		/// </summary>
		/// <param name="obj">The object to serialize.</param>
		/// <returns>
		/// </returns>
		public static SqlBinary FromObject(object obj) {
			using (var stream = new MemoryStream(1024 * 3)) {
				var formatter = new BinaryFormatter();
				formatter.Serialize(stream, obj);
				stream.Flush();
				stream.Seek(0, SeekOrigin.Begin);

				return new SqlBinary(stream.ToArray());
			}
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
	}
}