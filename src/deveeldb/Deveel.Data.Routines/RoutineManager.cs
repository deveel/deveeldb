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
using System.Linq;
using System.Text;

using Deveel.Data;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;
using Deveel.Data.Sql.Types;

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

		public ITableContainer TableContainer {
			get { return new RoutinesTableContainer(transaction); }
		}

		private ITable FindEntry(ObjectName routineName) {
			var table = transaction.GetTable(SystemSchema.RoutineTableName);

			var schemav = table.GetResolvedColumnName(1);
			var namev = table.GetResolvedColumnName(2);

			using (var session = new SystemSession(transaction)) {
				using (var context = session.CreateQuery()) {
					var t = table.SimpleSelect(context, namev, SqlExpressionType.Equal,
						SqlExpression.Constant(Field.String(routineName.Name)));
					t = t.ExhaustiveSelect(context,
						SqlExpression.Equal(SqlExpression.Reference(schemav),
							SqlExpression.Constant(Field.String(routineName.ParentName))));

					// This should be at most 1 row in size
					if (t.RowCount > 1)
						throw new Exception("Assert failed: multiple procedure names for " + routineName);

					// Return the entries found.
					return t;
				}
			}
		}

		private ITable GetParameters(Field id) {
			var table = transaction.GetTable(SystemSchema.RoutineParameterTableName);
			return table.SelectEqual(0, id);
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
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.String());
			tableInfo.AddColumn("location", PrimitiveTypes.String());
			tableInfo.AddColumn("body", PrimitiveTypes.Binary());
			tableInfo.AddColumn("return_type", PrimitiveTypes.String());
			tableInfo.AddColumn("username", PrimitiveTypes.String());
			transaction.CreateTable(tableInfo);

			// SYSTEM.ROUTINE_PARAM
			tableInfo = new TableInfo(SystemSchema.RoutineParameterTableName);
			tableInfo.AddColumn("routine_id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("arg_name", PrimitiveTypes.String());
			tableInfo.AddColumn("arg_type", PrimitiveTypes.String());
			tableInfo.AddColumn("arg_attrs", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("in_out", PrimitiveTypes.String());
			tableInfo.AddColumn("offset", PrimitiveTypes.Integer());
			transaction.CreateTable(tableInfo);

			var fkCol = new[] {"routine_id"};
			var refCol = new[] {"id"};
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
			string routineType = null;

			if (routineInfo.Body != null) {
				if (routineInfo.RoutineType == RoutineType.Function) {
					routineType = FunctionType;
				} else if (routineInfo.RoutineType == RoutineType.Procedure) {
					routineType = ProcedureType;
				}
			} else if (routineInfo.ExternalType != null) {
				if (routineInfo.RoutineType == RoutineType.Function) {
					routineType = ExtrernalFunctionType;
				} else if (routineInfo.RoutineType == RoutineType.Procedure) {
					routineType = ExternalProcedureType;
				}
			} else {
				throw new ArgumentException("The routine info is invalid.");
			}

			if (String.IsNullOrEmpty(routineType))
				throw new InvalidOperationException("Could not determine the kind of routine.");

			var id = transaction.NextTableId(SystemSchema.RoutineTableName);

			var routine = transaction.GetMutableTable(SystemSchema.RoutineTableName);
			var routineParams = transaction.GetMutableTable(SystemSchema.RoutineParameterTableName);

			var row = routine.NewRow();
			row.SetValue(0, id);
			row.SetValue(1, routineInfo.RoutineName.ParentName);
			row.SetValue(2, routineInfo.RoutineName.Name);
			row.SetValue(3, routineType);

			if (routineType == ExternalProcedureType ||
			    routineType == ExtrernalFunctionType) {
				var location = FormLocation(routineInfo.ExternalType, routineInfo.ExternalMethodName);
				row.SetValue(4, location);
			} else {
				var bin = SqlBinary.ToBinary(routineInfo.Body);
				row.SetValue(5, bin);
			}

			if (routineInfo is FunctionInfo) {
				var returnType = ((FunctionInfo) routineInfo).ReturnType.ToString();
				row.SetValue(6, returnType);
			}

			row.SetValue(7, routineInfo.Owner);

			if (routineInfo.Parameters != null) {
				foreach (var parameter in routineInfo.Parameters) {
					var prow = routineParams.NewRow();
					prow.SetValue(0, id);
					prow.SetValue(1, parameter.Name);

					var argType = parameter.Type.ToString();
					prow.SetValue(2, argType);

					var attrs = new SqlNumber((int)parameter.Attributes);
					prow.SetValue(3, attrs);

					var dir = new SqlNumber((int)parameter.Direction);
					prow.SetValue(4, dir);

					prow.SetValue(5, parameter.Offset);
				}
			}
		}

		bool IObjectManager.RealObjectExists(ObjectName objName) {
			return RoutineExists(objName);
		}

		bool IObjectManager.ObjectExists(ObjectName objName) {
			return RoutineExists(objName);
		}

		public bool RoutineExists(ObjectName routineName) {
			var table = transaction.GetTable(SystemSchema.RoutineTableName);
			var schemav = table.GetResolvedColumnName(1);
			var namev = table.GetResolvedColumnName(2);

			using (var session = new SystemSession(transaction)) {
				using (var context = session.CreateQuery()) {
					var t = table.SimpleSelect(context, namev, SqlExpressionType.Equal,
						SqlExpression.Constant(Field.String(routineName.Name)));
					t = t.ExhaustiveSelect(context,
						SqlExpression.Equal(SqlExpression.Reference(schemav),
							SqlExpression.Constant(Field.String(routineName.ParentName))));

					return t.RowCount == 1;
				}
			}
		}

		IDbObject IObjectManager.GetObject(ObjectName objName) {
			return GetRoutine(objName);
		}

		private RoutineParameter[] CreateParameters(ITable result) {
			var list = new List<RoutineParameter>();

			foreach (var row in result) {
				var paramName = row.GetValue(1).Value.ToString();
				var paramTypeString = row.GetValue(2).Value.ToString();

				var paramType = transaction.Context.ResolveType(paramTypeString);

				var attrs = (ParameterAttributes) ((SqlNumber) row.GetValue(3).Value).ToInt32();
				var direction = (ParameterDirection) ((SqlNumber) row.GetValue(4).Value).ToInt32();
				var offset = ((SqlNumber) row.GetValue(5).Value).ToInt32();

				list.Add(new RoutineParameter(paramName, paramType, direction, attrs) {
					Offset = offset
				});
			}

			return list.OrderBy(x => x.Offset).ToArray();
		}

		private static void ParseLocation(string externLocation, out Type externType, out string externMethod) {
			if (String.IsNullOrEmpty(externLocation))
				throw new ArgumentNullException("externLocation");

			try {
				string typeString;
				var delim = externLocation.LastIndexOf('.');
				if (delim == -1) {
					typeString = externLocation;
					externMethod = null;
				} else {
					typeString = externLocation.Substring(0, delim);
					externMethod = externLocation.Substring(delim + 1);
				}

				if (String.IsNullOrEmpty(typeString))
					throw new FormatException();

				externType = Type.GetType(typeString, true);
			} catch (FormatException ex) {
				throw new FormatException(String.Format("Location '{0}' is not in the right format.", externLocation), ex);
			} catch (Exception ex) {
				throw new FormatException(String.Format("Error while parsing extern location '{0}'.", externLocation), ex);
			}		
		}

		private static string FormLocation(Type externType, string externMethod) {
			var sb = new StringBuilder();
			sb.Append(externType.FullName);
			if (!String.IsNullOrEmpty(externMethod))
				sb.Append('.').Append(externMethod);

			return sb.ToString();
		}

		public IRoutine GetRoutine(ObjectName routineName) {
			var t = FindEntry(routineName);
			if (t == null || t.RowCount == 0)
				return null;

			var id = t.GetValue(0, 0);
			var schemaName = t.GetValue(0, 1).Value.ToString();
			var name = t.GetValue(0, 2).Value.ToString();

			var fullName = new ObjectName(ObjectName.Parse(schemaName), name);

			var t2 = GetParameters(id);

			var parameters = CreateParameters(t2);

			var routineType = t.GetValue(0, 2).Value.ToString();
			var returnTypeString = t.GetValue(0, 6).Value.ToString();
			var owner = t.GetValue(0, 7).Value.ToString();

			RoutineInfo info;

			if (routineType == FunctionType ||
				routineType == ExtrernalFunctionType) {
				var returnType = transaction.Context.ResolveType(returnTypeString);
				var funcType = routineType == FunctionType ? Routines.FunctionType.UserDefined : Routines.FunctionType.External;
				info = new FunctionInfo(fullName, parameters, returnType, funcType);
			} else if (routineType == ProcedureType ||
			           routineType == ExternalProcedureType) {
				var procType = routineType == ExternalProcedureType
					? Routines.ProcedureType.External
					: Routines.ProcedureType.UserDefined;
				info = new ProcedureInfo(fullName, procType, parameters);
			} else {
				throw new InvalidOperationException(String.Format("Invalid routine type '{0}' found in database", routineType));
			}

			info.Owner = owner;

			if (routineType == ExternalProcedureType ||
			    routineType == ExtrernalFunctionType) {
				var location = t.GetValue(0, 4).Value.ToString();
				Type externType;
				string externMethod;
				ParseLocation(location, out externType, out externMethod);

				info.ExternalType = externType;
				info.ExternalMethodName = externMethod;
			} else {
				var bodyBin = (SqlBinary) t.GetValue(0, 5).Value;
				info.Body = bodyBin.ToObject<PlSqlBlockStatement>();
			}

			if (routineType == ExternalProcedureType)
				return new ExternalProcedure((ProcedureInfo) info);
			if (routineType == ExtrernalFunctionType)
				return new ExternalFunction((FunctionInfo) info);
			if (routineType == FunctionType)
				return new UserFunction((FunctionInfo)info);
			if (routineType == ProcedureType)
				return new UserProcedure((ProcedureInfo) info);

			throw new InvalidOperationException();
		}

		bool IObjectManager.AlterObject(IObjectInfo objInfo) {
			var routineInfo = objInfo as RoutineInfo;
			if (routineInfo == null)
				throw new ArgumentException();

			return ReplaceRoutine(routineInfo);
		}

		bool IObjectManager.DropObject(ObjectName objName) {
			return DropRoutine(objName);
		}

		public bool ReplaceRoutine(RoutineInfo routineInfo) {
			if (!RemoveRoutine(routineInfo.RoutineName))
				return false;

			CreateRoutine(routineInfo);
			return true;
		}

		private bool RemoveRoutine(ObjectName routineName) {
			var routine = transaction.GetMutableTable(SystemSchema.RoutineTableName);
			var routineParam = transaction.GetMutableTable(SystemSchema.RoutineParameterTableName);

			var list = routine.SelectRowsEqual(2, Field.VarChar(routineName.Name), 1, Field.VarChar(routineName.ParentName));

			bool deleted = false;

			foreach (var rowIndex in list) {
				var sid = routine.GetValue(rowIndex, 0);
				var list2 = routineParam.SelectRowsEqual(0, sid);
				foreach (int rowIndex2 in list2) {
					routineParam.RemoveRow(rowIndex2);
				}

				routine.RemoveRow(rowIndex);
				deleted = true;
			}

			return deleted;
		}

		public bool DropRoutine(ObjectName objName) {
			return RemoveRoutine(objName);
		}

		public ObjectName ResolveName(ObjectName objName, bool ignoreCase) {
			var routine = transaction.GetMutableTable(SystemSchema.RoutineTableName);

			var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

			foreach (var row in routine) {
				var schemaName = row.GetValue(1).Value.ToString();
				var name = row.GetValue(2).Value.ToString();

				if (String.Equals(schemaName, objName.ParentName, comparison) &&
					String.Equals(name, objName.Name, comparison))
					return	new ObjectName(ObjectName.Parse(schemaName), name);
			}

			return null;
		}

		public IRoutine ResolveRoutine(Invoke invoke, IRequest context) {
			//TODO: support also invoke match ...
			return GetRoutine(invoke.RoutineName);
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

				return new RoutineTable(Transaction.Database.Context, tableInfo) {
					Type = type,
					Location = location,
					ReturnType = returnType,
					ParameterTypes = paramTypes,
					Owner = owner
				};
			}

			private Field GetParameterTypes(string schema, string name) {
				var table = Transaction.GetTable(SystemSchema.RoutineParameterTableName);
				var rows = table.SelectRowsEqual(1, Field.String(name), 0, Field.String(schema));
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

				public RoutineTable(IDatabaseContext dbContext, TableInfo tableInfo) 
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

		#endregion
	}
}
