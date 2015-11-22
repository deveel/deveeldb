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
using System.Collections.Generic;
using System.Text;

using Deveel.Data;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;
using Deveel.Data.Types;

namespace Deveel.Data.Routines {
	public sealed class RoutineManager : IObjectManager, IRoutineResolver {
		private const string ProcedureType = "procedure";
		private const string ExternalProcedureType = "ext_procedure";
		private const string FunctionType = "function";
		private const string ExtrernalFunctionType = "ext_function";

		private ITransaction transaction;

		public RoutineManager(ITransaction transaction) {
			if (transaction == null)
				throw new ArgumentNullException("transaction");

			this.transaction = transaction;
		}

		private ITable FindEntry(Table table, ObjectName routineName) {
			var schemav = table.GetResolvedColumnName(0);
			var namev = table.GetResolvedColumnName(1);

			using (var session = new SystemUserSession(transaction)) {
				using (var context = session.CreateQuery()) {
					var t = table.SimpleSelect(context, namev, SqlExpressionType.Equal,
						SqlExpression.Constant(DataObject.String(routineName.Name)));
					t = t.ExhaustiveSelect(context,
						SqlExpression.Equal(SqlExpression.Reference(schemav),
							SqlExpression.Constant(DataObject.String(routineName.ParentName))));

					// This should be at most 1 row in size
					if (t.RowCount > 1)
						throw new Exception("Assert failed: multiple procedure names for " + routineName);

					// Return the entries found.
					return t;
				}
			}
		}

		public void Dispose() {
			transaction = null;
		}

		DbObjectType IObjectManager.ObjectType {
			get { return DbObjectType.Routine; }
		}

		public void Create() {
			// SYSTEM.ROUTINE
			var tableInfo = new TableInfo(SystemSchema.RoutineTableName);
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.String());
			tableInfo.AddColumn("location", PrimitiveTypes.String());
			tableInfo.AddColumn("return_type", PrimitiveTypes.String());
			tableInfo.AddColumn("username", PrimitiveTypes.String());
			transaction.CreateTable(tableInfo);

			// SYSTEM.ROUTINE_PARAM
			tableInfo = new TableInfo(SystemSchema.RoutineParameterTableName);
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("arg_name", PrimitiveTypes.String());
			tableInfo.AddColumn("arg_type", PrimitiveTypes.String());
			tableInfo.AddColumn("in_out", PrimitiveTypes.String());
			tableInfo.AddColumn("offset", PrimitiveTypes.Integer());
			transaction.CreateTable(tableInfo);

			var fkCol = new[] {"routine_schema", "routine_name"};
			var refCol = new[] {"schema", "name"};
			const ForeignKeyAction onUpdate = ForeignKeyAction.NoAction;
			const ForeignKeyAction onDelete = ForeignKeyAction.Cascade;

			transaction.AddForeignKey(SystemSchema.RoutineParameterTableName, fkCol, SystemSchema.RoutineTableName, refCol,
				onDelete, onUpdate, "ROUTINE_PARAMS_FK");
		}

		void IObjectManager.CreateObject(IObjectInfo objInfo) {
			if (objInfo == null)
				throw new ArgumentNullException("objInfo");

			var routineInfo = objInfo as RoutineInfo;
			if (routineInfo == null)
				throw new ArgumentException();

			CreateRoutine(routineInfo);
		}

		public void CreateRoutine(RoutineInfo routineInfo) {
			throw new NotImplementedException();
		}

		bool IObjectManager.RealObjectExists(ObjectName objName) {
			return RoutineExists(objName);
		}

		bool IObjectManager.ObjectExists(ObjectName objName) {
			return RoutineExists(objName);
		}

		public bool RoutineExists(ObjectName objName) {
			// TODO: implement
			return false;
		}

		IDbObject IObjectManager.GetObject(ObjectName objName) {
			return GetRoutine(objName);
		}

		public IRoutine GetRoutine(ObjectName routineName) {
			// TODO: implement!
			return null;
		}

		bool IObjectManager.AlterObject(IObjectInfo objInfo) {
			var routineInfo = objInfo as RoutineInfo;
			if (routineInfo == null)
				throw new ArgumentException();

			return AlterRoutine(routineInfo);
		}

		bool IObjectManager.DropObject(ObjectName objName) {
			return DropRoutine(objName);
		}

		public bool AlterRoutine(RoutineInfo routineInfo) {
			// TODO: implement
			return false;
		}

		public bool DropRoutine(ObjectName objName) {
			// TODO: implement
			return false;
		}

		public ObjectName ResolveName(ObjectName objName, bool ignoreCase) {
			throw new NotImplementedException();
		}

		public IRoutine ResolveRoutine(Invoke request, IQuery context) {
			// TODO: implement
			return null;
		}

		#region RoutinesTableContainer

		class RoutinesTableContainer : SystemTableContainer {
			public RoutinesTableContainer(ITransaction transaction)
				: base(transaction, SystemSchema.RoutineTableName) {
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
				if (String.Equals(typeString, FunctionType, StringComparison.OrdinalIgnoreCase) ||
					String.Equals(typeString, ExternalProcedureType, StringComparison.OrdinalIgnoreCase))
					return TableTypes.Function;

				if (String.Equals(typeString, ProcedureType, StringComparison.OrdinalIgnoreCase) ||
				    String.Equals(typeString, ExternalProcedureType, StringComparison.OrdinalIgnoreCase))
					return TableTypes.Procedure;

				throw new InvalidOperationException(String.Format("The type {0} is invalid as routine table type.", typeString));
			}

			public override ITable GetTable(int offset) {
				var table = Transaction.GetTable(SystemSchema.RoutineTableName);
				var rowE = table.GetEnumerator();
				int p = 0;
				int i;
				int rowI = -1;
				while (rowE.MoveNext()) {
					i = rowE.Current.RowId.RowNumber;
					if (p == offset) {
						rowI = i;
					} else {
						++p;
					}
				}

				if (p != offset)
					throw new ArgumentOutOfRangeException("offset");

				string schema = table.GetValue(rowI, 0).Value.ToString();
				string name = table.GetValue(rowI, 1).Value.ToString();

				var paramTypes = GetParameterTypes(schema, name);

				var tableInfo = CreateTableInfo(schema, name);
				var type = table.GetValue(rowI, 2);
				var location = table.GetValue(rowI, 3);
				var returnType = table.GetValue(rowI, 4);				
				var owner = table.GetValue(rowI, 5);

				return new RoutineTable(Transaction.Database.DatabaseContext, tableInfo) {
					Type = type,
					Location = location,
					ReturnType = returnType,
					ParameterTypes = paramTypes,
					Owner = owner
				};
			}

			private DataObject GetParameterTypes(string schema, string name) {
				var table = Transaction.GetTable(SystemSchema.RoutineParameterTableName);
				var rows = table.SelectRowsEqual(1, DataObject.String(name), 0, DataObject.String(schema));
				var types = new List<string>();

				foreach (var rowIndex in rows) {
					var argName = table.GetValue(rowIndex, 1);
					var argType = table.GetValue(rowIndex, 2);
					var inOut = table.GetValue(rowIndex, 3);

					var paramString = BuildParameterString(argName, argType, inOut);

					types.Add(paramString);
				}

				var args = String.Join(", ", types.ToArray());
				return DataObject.String(args);
			}

			private string BuildParameterString(DataObject argName, DataObject argType, DataObject inOut) {
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

				public RoutineTable(IDatabaseContext dbContext, TableInfo tableInfo) 
					: base(dbContext) {
					this.tableInfo = tableInfo;
				}

				public override TableInfo TableInfo {
					get { return tableInfo; }
				}

				public DataObject Type { get; set; }

				public DataObject Location { get; set; }

				public DataObject ReturnType { get; set; }

				public DataObject ParameterTypes { get; set; }

				public DataObject Owner { get; set; }

				public override int RowCount {
					get { return 1; }
				}

				public override DataObject GetValue(long rowNumber, int columnOffset) {
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

		#endregion
	}
}
