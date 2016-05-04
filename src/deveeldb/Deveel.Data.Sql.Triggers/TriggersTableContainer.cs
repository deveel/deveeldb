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
using Deveel.Data.Sql.Types;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Triggers {
	class TriggersTableContainer : TableContainerBase {
		public TriggersTableContainer(ITransaction transaction) 
			: base(transaction,TriggerManager.TriggerTableName) {
		}

		public override TableInfo GetTableInfo(int offset) {
			var triggerName = GetTableName(offset);
			return CreateTableInfo(triggerName.ParentName, triggerName.Name);
		}

		public override string GetTableType(int offset) {
			return TableTypes.Trigger;
		}

		private static TableInfo CreateTableInfo(string schema, string name) {
			var tableInfo = new TableInfo(new ObjectName(new ObjectName(schema), name));

			tableInfo.AddColumn("type", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("on_object", PrimitiveTypes.String());
			tableInfo.AddColumn("routine_name", PrimitiveTypes.String());
			tableInfo.AddColumn("param_args", PrimitiveTypes.String());
			tableInfo.AddColumn("owner", PrimitiveTypes.String());

			return tableInfo.AsReadOnly();
		}


		public override ITable GetTable(int offset) {
			var table = Transaction.GetTable(TriggerManager.TriggerTableName);
			var enumerator = table.GetEnumerator();
			int p = 0;
			int i;
			int rowIndex = -1;
			while (enumerator.MoveNext()) {
				i = enumerator.Current.RowId.RowNumber;
				if (p == offset) {
					rowIndex = i;
				} else {
					++p;
				}
			}

			if (p != offset)
				throw new ArgumentOutOfRangeException("offset");

			var schema = table.GetValue(rowIndex, 0).Value.ToString();
			var name = table.GetValue(rowIndex, 1).Value.ToString();

			var tableInfo = CreateTableInfo(schema, name);

			var type = table.GetValue(rowIndex, 2);
			var tableName = table.GetValue(rowIndex, 3);
			var routine = table.GetValue(rowIndex, 4);
			var args = table.GetValue(rowIndex, 5);
			var owner = table.GetValue(rowIndex, 6);

			return new TriggerTable(Transaction, tableInfo) {
				Type = type,
				TableName = tableName,
				Routine = routine,
				Arguments = args,
				Owner = owner
			};
		}

		#region TriggerTable

		class TriggerTable : GeneratedTable {
			private TableInfo tableInfo;

			public TriggerTable(ITransaction transaction, TableInfo tableInfo) 
				: base(transaction.Database.Context) {
				this.tableInfo = tableInfo;
			}

			public override TableInfo TableInfo {
				get { return tableInfo; }
			}

			public Field Type { get; set; }

			public Field TableName { get; set; }

			public Field Routine { get; set; }

			public Field Arguments { get; set; }

			public Field Owner { get; set; }

			public override int RowCount {
				get { return 1; }
			}

			public override Field GetValue(long rowNumber, int columnOffset) {
				if (rowNumber > 0)
					throw new ArgumentOutOfRangeException("rowNumber");

				switch (columnOffset) {
					case 0:
						return Type;
					case 1:
						return TableName;
					case 2:
						return Routine;
					case 3:
						return Arguments;
					case 4:
						return Owner;
					default:
						throw new ArgumentOutOfRangeException("columnOffset");
				}
			}
		}

		#endregion
	}
}