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
using System.Runtime.Serialization;
using System.Text;

using Deveel.Data.Store;

namespace Deveel.Data.Sql.Objects {
	public sealed class SqlLongString : ISqlString, IDisposable {
		private readonly ILargeObject largeObject;
		private readonly Encoding encoding;

		public SqlLongString(ILargeObject largeObject, int codePage) {
			this.largeObject = largeObject;
			encoding = Encoding.GetEncoding(codePage);
		}

		~SqlLongString() {
			Dispose(false);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (largeObject != null)
					largeObject.Dispose();
			}
		}

		int IComparable.CompareTo(object obj) {
			if (!(obj is SqlLongString))
				throw new ArgumentException();

			return CompareTo((SqlLongString) obj);
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			if (!(other is ISqlString))
				throw new ArgumentException();

			return CompareTo((ISqlString) other);
		}

		public bool IsNull {
			get { return largeObject == null; }
		}

		public bool IsComparableTo(ISqlObject other) {
			return other is ISqlString;
		}

		public int CompareTo(ISqlString other) {
			if (other == null)
				throw new ArgumentNullException("other");

			if (IsNull && other.IsNull)
				return 0;
			if (IsNull)
				return -1;
			if (other.IsNull)
				return 1;

			var r1 = GetInput();
			var r2 = other.GetInput();

			int c1 = 0, c2 = 0;

			// read one char at a time and compare them
			// until we reach end of one of the strings
			while ((c1 = r1.Read()) != -1 &&
			       (c2 = r2.Read()) != -1) {
				var result = ((char) c1).CompareTo((char) c2);
				if (result != 0) 
					return result;
			}

			// if both are -1 then strings have the same length and 
			// consist of same chars so they are equal
			return c1 == -1 && c2 == -1 ? 0 : (c1 == -1 ? -1 : 1);
		}

		public IEnumerator<char> GetEnumerator() {
			throw new NotImplementedException();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public int CodePage { get; private set; }

		public long Length { get; private set; }

		public TextReader GetInput() {
			if (largeObject == null)
				return TextReader.Null;

			return new StreamReader(new ObjectStream(largeObject), encoding);
		}

		public TextWriter GetOutput() {
			if (largeObject == null)
				return TextWriter.Null;

			return new StreamWriter(new ObjectStream(largeObject), encoding);
		}

		public static SqlLongString Unicode(ILargeObject obj) {
			return new SqlLongString(obj, Encoding.Unicode.CodePage);
		}
	}
}