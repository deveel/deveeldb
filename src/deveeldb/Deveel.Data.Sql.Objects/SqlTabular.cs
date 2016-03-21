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

using Deveel.Data;
using Deveel.Data.Index;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Objects {
	public sealed class SqlTabular : ITable, ISqlObject {
		private ITable table;

		private SqlTabular(ITable table) {
			this.table = table;
		}

		int IComparable.CompareTo(object obj) {
			throw new NotSupportedException();
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			throw new NotSupportedException();
		}

		public bool IsNull {
			get { return table == null; }
		}

		private void AssertNotNull() {
			if (table == null)
				throw new NullReferenceException("The object is null.");
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return false;
		}

		public static SqlTabular From(ITable table) {
			return new SqlTabular(table);
		}

		ObjectName IDbObject.FullName {
			get {
				AssertNotNull();
				return table.FullName;
			}
		}

		DbObjectType IDbObject.ObjectType {
			get { return DbObjectType.Table; }
		}

		public IEnumerator<Row> GetEnumerator() {
			AssertNotNull();
			return table.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public void Dispose() {
			table = null;
		}

		IContext ITable.Context {
			get {
				AssertNotNull();
				return table.Context;
			}
		}

		TableInfo ITable.TableInfo {
			get {
				AssertNotNull();
				return table.TableInfo;
			}
		}

		public int RowCount {
			get {
				AssertNotNull();
				return table.RowCount;
			}
		}

		public Field GetValue(long rowNumber, int columnOffset) {
			AssertNotNull();
			return table.GetValue(rowNumber, columnOffset);
		}

		ColumnIndex ITable.GetIndex(int columnOffset) {
			AssertNotNull();
			return table.GetIndex(columnOffset);
		}
	}
}
