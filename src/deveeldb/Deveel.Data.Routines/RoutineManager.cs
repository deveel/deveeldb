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
using System.Linq;
using System.Text;

using Deveel.Data.Diagnostics;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
	public sealed class RoutineManager : IObjectManager, IRoutineResolver {
		internal const string ProcedureType = "procedure";
		internal const string ExternalProcedureType = "ext_procedure";
		internal const string FunctionType = "function";
		internal const string ExtrernalFunctionType = "ext_function";

		private ITransaction transaction;
		private Dictionary<ObjectName, IRoutine> routinesCache;

		public RoutineManager(ITransaction transaction) {
			if (transaction == null)
				throw new ArgumentNullException("transaction");

			routinesCache = new Dictionary<ObjectName, IRoutine>(ObjectNameEqualityComparer.CaseInsensitive);

			this.transaction = transaction;
			this.transaction.Context.RouteImmediate<TransactionEvent>(OnTransactionEnd, e => e.EventType != TransactionEventType.Begin);
		}

		public static readonly ObjectName RoutineTableName = new ObjectName(SystemSchema.SchemaName, "routine");

		public static readonly ObjectName RoutineParameterTableName = new ObjectName(SystemSchema.SchemaName, "routine_params");

		private void OnTransactionEnd(TransactionEvent @event) {
			routinesCache.Clear();
		}

		private ITable FindEntry(ObjectName routineName) {
			var table = transaction.GetTable(RoutineTableName);

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
			var table = transaction.GetTable(RoutineParameterTableName);
			return table.SelectEqual(0, id);
		}

		public void Dispose() {
			transaction = null;
		}

		DbObjectType IObjectManager.ObjectType {
			get { return DbObjectType.Routine; }
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
			try {
				string routineType = null;

				if (routineInfo is FunctionInfo) {
					if (routineInfo is ExternalFunctionInfo) {
						routineType = ExtrernalFunctionType;
					} else if (routineInfo is PlSqlFunctionInfo) {
						routineType = FunctionType;
					}
				} else if (routineInfo is ProcedureInfo) {
					if (routineInfo is PlSqlProcedureInfo) {
						routineType = ProcedureType;
					} else if (routineInfo is ExternalProcedureInfo) {
						routineType = ExternalProcedureType;
					}
				} else {
					throw new ArgumentException();
				}

				if (String.IsNullOrEmpty(routineType))
					throw new InvalidOperationException("Could not determine the kind of routine.");

				var id = transaction.NextTableId(RoutineTableName);

				var routine = transaction.GetMutableTable(RoutineTableName);
				var routineParams = transaction.GetMutableTable(RoutineParameterTableName);

				var row = routine.NewRow();
				row.SetValue(0, id);
				row.SetValue(1, routineInfo.RoutineName.ParentName);
				row.SetValue(2, routineInfo.RoutineName.Name);
				row.SetValue(3, routineType);

				if (routineType == ExternalProcedureType) {
					var extProcedure = (ExternalProcedureInfo)routineInfo;
					var location = extProcedure.ExternalRef.ToString();
					row.SetValue(4, location);
				} else if (routineType == ExtrernalFunctionType) {
					var extFunction = (ExternalFunctionInfo)routineInfo;
					var location = extFunction.ExternalRef.ToString();
					row.SetValue(4, location);
				} else if (routineType == ProcedureType) {
					var plsqlProcedure = (PlSqlProcedureInfo)routineInfo;
					var bin = SqlBinary.ToBinary(plsqlProcedure.Body);
					row.SetValue(5, bin);
				} else if (routineType == FunctionType) {
					var plsqlFunction = (PlSqlFunctionInfo)routineInfo;
					var bin = SqlBinary.ToBinary(plsqlFunction.Body);
					row.SetValue(5, bin);
				}

				if (routineInfo is FunctionInfo) {
					var returnType = ((FunctionInfo)routineInfo).ReturnType.ToString();
					row.SetValue(6, returnType);
				}

				row.SetValue(7, routineInfo.Owner);
				routine.AddRow(row);

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

						routineParams.AddRow(prow);
					}
				}

				transaction.OnObjectCreated(DbObjectType.Routine, routineInfo.RoutineName);
			} finally {
				routinesCache.Clear();
			}
		}

		bool IObjectManager.RealObjectExists(ObjectName objName) {
			return RoutineExists(objName);
		}

		bool IObjectManager.ObjectExists(ObjectName objName) {
			return RoutineExists(objName);
		}

		public bool RoutineExists(ObjectName routineName) {
			if (routinesCache.ContainsKey(routineName))
				return true;

			var table = transaction.GetTable(RoutineTableName);
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

				var paramType = SqlType.Parse(transaction.Context, paramTypeString);

				var attrs = (ParameterAttributes) ((SqlNumber) row.GetValue(3).Value).ToInt32();
				var direction = (ParameterDirection) ((SqlNumber) row.GetValue(4).Value).ToInt32();
				var offset = ((SqlNumber) row.GetValue(5).Value).ToInt32();

				list.Add(new RoutineParameter(paramName, paramType, direction, attrs) {
					Offset = offset
				});
			}

			return list.OrderBy(x => x.Offset).ToArray();
		}

		public IRoutine GetRoutine(ObjectName routineName) {
			IRoutine result;
			if (!routinesCache.TryGetValue(routineName, out result)) {
				var t = FindEntry(routineName);
				if (t == null || t.RowCount == 0)
					return null;

				var id = t.GetValue(0, 0);
				var schemaName = t.GetValue(0, 1).Value.ToString();
				var name = t.GetValue(0, 2).Value.ToString();

				var fullName = new ObjectName(ObjectName.Parse(schemaName), name);

				var t2 = GetParameters(id);

				var parameters = CreateParameters(t2);

				var routineType = t.GetValue(0, 3).Value.ToString();
				var returnTypeString = t.GetValue(0, 6).Value.ToString();
				var owner = t.GetValue(0, 7).Value.ToString();

				RoutineInfo info;

				SqlType returnType = null;

				if (routineType == FunctionType ||
				    routineType == ExtrernalFunctionType) {
					returnType = transaction.Context.ResolveType(returnTypeString);
				}

				SqlStatement body = null;
				ExternalRef externalRef = null;

				if (routineType == FunctionType ||
				    routineType == ProcedureType) {
					var bodyBin = (SqlBinary) t.GetValue(0, 5).Value;
					body = bodyBin.ToObject<PlSqlBlockStatement>();
				} else if (routineType == ExtrernalFunctionType ||
				           routineType == ExternalProcedureType) {
					var location = t.GetValue(0, 4).Value.ToString();

					if (!ExternalRef.TryParse(location, out externalRef))
						throw new InvalidOperationException(String.Format("The location stored for function '{0}' is invalid: {1}.",
							routineName, location));
				}

				if (routineType == FunctionType) {
					info = new PlSqlFunctionInfo(fullName, parameters, returnType, body);
				} else if (routineType == ProcedureType) {
					info = new PlSqlProcedureInfo(fullName, parameters, body);
				} else if (routineType == ExtrernalFunctionType) {
					info = new ExternalFunctionInfo(fullName, parameters, returnType, externalRef);
				} else if (routineType == ExternalProcedureType) {
					info = new ExternalProcedureInfo(fullName, parameters, externalRef);
				} else {
					throw new InvalidOperationException(String.Format("Invalid routine type '{0}' found in database", routineType));
				}

				info.Owner = owner;

				if (info is PlSqlFunctionInfo) {
					result = new PlSqlFunction((PlSqlFunctionInfo) info);
				} else if (info is PlSqlProcedureInfo) {
					result = new PlSqlProcedure((PlSqlProcedureInfo) info);
				} else if (info is ExternalFunctionInfo) {
					result = new ExternalFunction((ExternalFunctionInfo) info);
				} else {
					result = new ExternalProcedure((ExternalProcedureInfo) info);
				}

				routinesCache[fullName] = result;
			}

			return result;
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
			var routine = transaction.GetMutableTable(RoutineTableName);
			var routineParam = transaction.GetMutableTable(RoutineParameterTableName);

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

			if (deleted)
				routinesCache.Remove(routineName);

			return deleted;
		}

		public bool DropRoutine(ObjectName objName) {
			return RemoveRoutine(objName);
		}

		public ObjectName ResolveName(ObjectName objName, bool ignoreCase) {
			var routine = transaction.GetMutableTable(RoutineTableName);

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
	}
}
