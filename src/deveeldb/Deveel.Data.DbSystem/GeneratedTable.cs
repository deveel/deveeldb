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
//

using System;
using System.Collections;
using System.Collections.Generic;

using Deveel.Data.Index;
using Deveel.Data.Sql;

namespace Deveel.Data.DbSystem {
	abstract class GeneratedTable : ITable {
		protected GeneratedTable(ISystemContext systemContext) {
			SystemContext = systemContext;
		}

		~GeneratedTable() {
			Dispose(false);
		}

		protected ISystemContext SystemContext { get; private set; }

		ObjectName IDbObject.FullName {
			get { return TableInfo.TableName; }
		}

		DbObjectType IDbObject.ObjectType {
			get { return DbObjectType.Table; }
		}

		public IEnumerator<Row> GetEnumerator() {
			return new SimpleRowEnumerator(this);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public abstract TableInfo TableInfo { get; }

		public abstract int RowCount { get; }

		public abstract DataObject GetValue(long rowNumber, int columnOffset);

		public virtual ColumnIndex GetIndex(int columnOffset) {
			return new BlindSearchIndex(this, columnOffset);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
		}
	}
}
