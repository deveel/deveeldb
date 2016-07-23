// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data.Store;

namespace Deveel.Data.Sql.Objects {
	public sealed class SqlLongBinary : ISqlBinary, IObjectRef, IDisposable {
		private ILargeObject largeObject;

		public static readonly SqlLongBinary Null = new SqlLongBinary(null);

		public SqlLongBinary(ILargeObject largeObject) {
			this.largeObject = largeObject;
		}

		~SqlLongBinary() {
			Dispose(false);
		}

		private void AssertNotDisposed() {
			if (largeObject == null)
				throw new ObjectDisposedException("SqlLongBinary");
		}

		public int CompareTo(object obj) {
			throw new NotImplementedException();
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			throw new NotSupportedException();
		}

		public bool IsNull {
			get { return largeObject == null; }
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return false;
		}

		public IEnumerator<byte> GetEnumerator() {
			AssertNotDisposed();
			throw new NotImplementedException();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public long Length {
			get {
				AssertNotDisposed();
				return largeObject.RawSize;
			}
		}

		public Stream GetInput() {
			AssertNotDisposed();
			return new ObjectStream(largeObject);
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

			largeObject = null;
		}

		public ObjectId ObjectId {
			get {
				AssertNotDisposed();
				return largeObject.Id;
			}
		}
	}
}