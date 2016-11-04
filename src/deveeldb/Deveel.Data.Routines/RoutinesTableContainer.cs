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
using System.Collections.Generic;
using System.Text;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;
using Deveel.Data.Transactions;

namespace Deveel.Data.Routines {
	class RoutinesTableContainer : TableContainerBase {
		public RoutinesTableContainer(ITransaction transaction)
			: base(transaction, RoutineManager.RoutineTableName) {
		}

		protected override int NameColumnOffset {
			get { return 2; }
		}

		protected override int SchemaColumnOffset {
			get { return 1; }
		}

		public RoutineManager Manager {
			get { return Transaction.GetObjectManager(DbObjectType.Routine) as RoutineManager; }
		}

		private static TableInfo CreateTableInfo(string schema, string name) {
			// Create the TableInfo that describes this entry
			var info = new TableInfo(new ObjectName(new ObjectName(schema), name));

			// Add column definitions
			info.AddColumn("type", PrimitiveTypes.String());
			info.AddColumn("location", PrimitiveTypes.String());
			info.AddColumn("return_type", PrimitiveTypes.String());
			info.AddColumn("param_args", PrimitiveTypes.String());
			info.AddColumn("owner", PrimitiveTypes.String());

			return info.AsReadOnly();
		}

		public override ObjectName GetTableName(int offset) {
			return Manager.NameAt(offset, base.GetTableName);
		}

		public override int FindByName(ObjectName name) {
			return Manager.OffsetOf(name, base.FindByName);
		}

		public override TableInfo GetTableInfo(int offset) {
			var tableName = GetTableName(offset);
			if (tableName == null)
				throw new ArgumentOutOfRangeException("offset");

			return CreateTableInfo(tableName.ParentName, tableName.Name);
		}

		public override string GetTableType(int offset) {
			var table = GetTable(offset);
			if (table == null)
				throw new ArgumentOutOfRangeException("offset");

			var typeString = table.GetValue(0, 0).Value.ToString();
			if (String.Equals(typeString, RoutineManager.FunctionType, StringComparison.OrdinalIgnoreCase) ||
				String.Equals(typeString, RoutineManager.ExtrernalFunctionType, StringComparison.OrdinalIgnoreCase))
				return TableTypes.Function;

			if (String.Equals(typeString, RoutineManager.ProcedureType, StringComparison.OrdinalIgnoreCase) ||
				String.Equals(typeString, RoutineManager.ExternalProcedureType, StringComparison.OrdinalIgnoreCase))
				return TableTypes.Procedure;

			throw new InvalidOperationException(String.Format("The type {0} is invalid as routine table type.", typeString));
		}

		public override ITable GetTable(int offset) {
			var table = Transaction.GetTable(RoutineManager.RoutineTableName);

			if (offset < 0 || offset >= table.RowCount)
				throw new ArgumentOutOfRangeException("offset");

			var routineId = ((SqlNumber) table.GetValue(offset, 0).Value).ToInt32();
			string schema = table.GetValue(offset, 1).Value.ToString();
			string name = table.GetValue(offset, 2).Value.ToString();

			var paramTypes = GetParameterTypes(routineId);

			var tableInfo = CreateTableInfo(schema, name);
			var type = table.GetValue(offset, 3);
			var location = table.GetValue(offset, 4);
			var returnType = table.GetValue(offset, 5);
			var owner = table.GetValue(offset, 6);

			return new RoutineTable(Transaction.Context, tableInfo) {
				Type = type,
				Location = location,
				ReturnType = returnType,
				ParameterTypes = paramTypes,
				Owner = owner
			};
		}

		private Field GetParameterTypes(int routineId) {
			var table = Transaction.GetTable(RoutineManager.RoutineParameterTableName);
			var rows = table.SelectRowsEqual(0, Field.Integer(routineId));
			var types = new List<string>();

			foreach (var rowIndex in rows) {
				var argName = table.GetValue(rowIndex, 1);
				var argType = table.GetValue(rowIndex, 2);
				var inOut = table.GetValue(rowIndex, 3);

				var paramString = BuildParameterString(argName, argType, inOut);

				types.Add(paramString);
			}

			var args = String.Join(", ", types.ToArray());
			return Field.String(args);
		}

		private string BuildParameterString(Field argName, Field argType, Field inOut) {
			var sb = new StringBuilder();
			sb.Append(argName.ToString());
			sb.Append(" ");
			sb.Append(argType.ToString());

			if (!inOut.IsNull)
				sb.Append(" ")
					.Append(inOut.Value);

			return sb.ToString();
		}

		#region RoutineTable

		class RoutineTable : GeneratedTable {
			private readonly TableInfo tableInfo;

			public RoutineTable(IContext dbContext, TableInfo tableInfo)
				: base(dbContext) {
				this.tableInfo = tableInfo;
			}

			public override TableInfo TableInfo {
				get { return tableInfo; }
			}

			public Field Type { get; set; }

			public Field Location { get; set; }

			public Field ReturnType { get; set; }

			public Field ParameterTypes { get; set; }

			public Field Owner { get; set; }

			public override int RowCount {
				get { return 1; }
			}

			public override Field GetValue(long rowNumber, int columnOffset) {
				if (rowNumber < 0 || rowNumber >= 1)
					throw new ArgumentOutOfRangeException("rowNumber");

				switch (columnOffset) {
					case 0:
						return Type;
					case 1:
						return Location;
					case 2:
						return ReturnType;
					case 3:
						return ParameterTypes;
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
