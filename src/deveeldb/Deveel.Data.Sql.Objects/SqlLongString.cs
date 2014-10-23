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

		public int CompareTo(ISqlObject other) {
			throw new NotImplementedException();
		}

		public bool IsNull {
			get { return largeObject == null; }
		}

		public bool IsComparableTo(ISqlObject other) {
			return other is ISqlString;
		}

		public int CompareTo(ISqlString other) {
			throw new NotImplementedException();
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
			throw new NotImplementedException();
		}

		public static SqlLongString Unicode(ILargeObject obj) {
			return new SqlLongString(obj, Encoding.Unicode.CodePage);
		}
	}
}