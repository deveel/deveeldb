// 
//  Copyright 2010-2014 Deveel
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
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Deveel.Data.Sql.Objects {
	[Serializable]
	public struct SqlBinary : ISqlBinary {
		private readonly byte[] bytes;

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

		public bool IsNull {
			get { return bytes == null; }
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return false;
		}

		public IEnumerator<byte> GetEnumerator() {
			throw new NotImplementedException();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public long Length {
			get { return bytes == null ? 0L : bytes.LongLength; }
		}

		public Stream GetInput() {
			if (IsNull)
				return Stream.Null;

			return new MemoryStream(bytes, false);
		}

		public object ToObject() {
			using (var input = GetInput()) {
				var fornatter = new BinaryFormatter();
				return fornatter.Deserialize(input);
			}
		}

		public static SqlBinary FromObject(object obj) {
			using (var stream = new MemoryStream(1024 * 3)) {
				var formatter = new BinaryFormatter();
				formatter.Serialize(stream, obj);
				stream.Flush();
				stream.Seek(0, SeekOrigin.Begin);

				return new SqlBinary(stream.ToArray());
			}
		}

		public byte[] ToByteArray() {
			if (bytes == null)
				return new byte[0];

			var destArray = new byte[bytes.Length];
			Array.Copy(bytes, 0, destArray, 0, bytes.Length);
			return destArray;
		}
	}
}