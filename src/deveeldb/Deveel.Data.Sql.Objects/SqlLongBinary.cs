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

using Deveel.Data.Store;

namespace Deveel.Data.Sql.Objects {
	public sealed class SqlLongBinary : ISqlBinary, IObjectRef, IDisposable {
		private readonly ILargeObject largeObject;

		public static readonly SqlLongBinary Null = new SqlLongBinary(null);

		public SqlLongBinary(ILargeObject largeObject) {
			this.largeObject = largeObject;
		}

		public int CompareTo(object obj) {
			throw new NotImplementedException();
		}

		public int CompareTo(ISqlObject other) {
			throw new NotImplementedException();
		}

		public bool IsNull {
			get { return largeObject == null; }
		}

		public bool IsComparableTo(ISqlObject other) {
			throw new NotImplementedException();
		}

		public IEnumerator<byte> GetEnumerator() {
			throw new NotImplementedException();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public long Length { get; private set; }

		public Stream GetInput() {
			throw new NotImplementedException();
		}

		public void Dispose() {
			throw new NotImplementedException();
		}

		public ObjectId ObjectId { get; private set; }
	}
}