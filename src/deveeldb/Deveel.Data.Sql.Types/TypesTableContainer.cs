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

using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Types {
	class TypesTableContainer : ITableContainer {
		private readonly ITransaction transaction;

		public TypesTableContainer(ITransaction transaction) {
			this.transaction = transaction;
		}

		public int TableCount {
			get {
				var table = transaction.GetTable(TypeManager.TypeTableName);
				return table == null ? 0 : table.RowCount;
			}
		}

		public int FindByName(ObjectName name) {
			if (name == null)
				throw new ArgumentNullException("name");

			if (!transaction.RealTableExists(TypeManager.TypeTableName))
				return -1;

			if (name.Parent == null)
				return -1;

			var typeSchema = Field.String(name.ParentName);
			var typeName = Field.String(name.Name);

			var table = transaction.GetTable(TypeManager.TypeTableName);

			int i = 0;
			foreach (var row in table) {
				if (row.GetValue(1).IsEqualTo(typeSchema) &&
				    row.GetValue(2).IsEqualTo(typeName))
					return i;

				i++;
			}

			return -1;
		}

		public ObjectName GetTableName(int offset) {
			if (!transaction.RealTableExists(TypeManager.TypeTableName))
				return null;

			var table = transaction.GetTable(TypeManager.TypeTableName);

			if (offset < 0 || offset >= table.RowCount)
				throw new ArgumentOutOfRangeException("offset");

			var schema = table.GetValue(offset, 1).Value.ToString();
			var name = table.GetValue(offset, 2).Value.ToString();

			return new ObjectName(ObjectName.Parse(schema), name);
		}

		public TableInfo GetTableInfo(int offset) {
			throw new NotImplementedException();
		}

		public string GetTableType(int offset) {
			return TableTypes.Type;
		}

		public bool ContainsTable(ObjectName name) {
			return FindByName(name) != -1;
		}

		public ITable GetTable(int offset) {
			throw new NotImplementedException();
		}

		#region TypeTable

		class TypeTable : GeneratedTable {
			private readonly TableInfo tableInfo;

			public TypeTable(ITransaction transaction, TableInfo tableInfo) 
				: base(transaction.Context) {
				this.tableInfo = tableInfo;
			}

			public override TableInfo TableInfo {
				get { return tableInfo; }
			}

			public override int RowCount {
				get { return 1; }
			}

			public override Field GetValue(long rowNumber, int columnOffset) {
				throw new NotImplementedException();
			}
		}

		#endregion
	}
}
