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
using System.Linq;
using System.Runtime.Serialization;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Tables;
using System.Text;

using Deveel.Data.Security;

namespace Deveel.Data.Sql.Statements {
	public sealed class InsertStatement : SqlStatement, IPlSqlStatement {
		public InsertStatement(ObjectName tableName, IEnumerable<SqlExpression[]> values) 
			: this(tableName, null, values) {
		}

		public InsertStatement(ObjectName tableName, IEnumerable<string> columnNames, IEnumerable<SqlExpression[]> values) {
			if (values == null)
				throw new ArgumentNullException("values");
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			TableName = tableName;
			ColumnNames = columnNames;
			Values = values;
		}

		public ObjectName TableName { get; private set; }

		public IEnumerable<string> ColumnNames { get; private set; } 

		public IEnumerable<SqlExpression[]> Values { get; private set; }

		protected override SqlStatement PrepareStatement(IRequest context) {
			var values = Values.ToArray();

			int firstLen = -1;
			for (int n = 0; n < values.Length; ++n) {
				var expList = (IList)values[n];
				if (firstLen == -1 || firstLen == expList.Count) {
					firstLen = expList.Count;
				} else {
					throw new InvalidOperationException("The insert data list varies in size.");
				}
			}

			var tableName = context.Access.ResolveTableName(TableName);

			var table = context.Access.GetTable(tableName);
			if (table == null)
				throw new ObjectNotFoundException(TableName);

			if (Values.Any(x => x.OfType<SqlQueryExpression>().Any()))
				throw new InvalidOperationException("Cannot set a value from a query.");

			var tableQueryInfo = context.Access.GetTableQueryInfo(tableName, null);
			var fromTable = new FromTableDirectSource(context.Query.IgnoreIdentifiersCase(), tableQueryInfo, "INSERT_TABLE", tableName, tableName);

			var columns = new string[0];
			if (ColumnNames != null)
				columns = ColumnNames.ToArray();

			if (columns.Length == 0) {
				columns = new string[table.TableInfo.ColumnCount];
				for (int i = 0; i < columns.Length; i++) {
					columns[i] = table.TableInfo[i].ColumnName;
				}
			}

			var colIndices = new int[columns.Length];
			var colResolved = new ObjectName[columns.Length];
			for (int i = 0; i < columns.Length; ++i) {
				var inVar = new ObjectName(columns[i]);
				var col = ResolveColumn(fromTable, inVar);
				int index = table.FindColumn(col);
				if (index == -1)
					throw new InvalidOperationException(String.Format("Cannot find column '{0}' in table '{1}'.", col, tableName));

				colIndices[i] = index;
				colResolved[i] = col;
			}


			var columnInfos = new List<ColumnInfo>();
			foreach (var name in columns) {
				var columnName = new ObjectName(tableName, name);
				var colIndex = table.FindColumn(columnName);
				if (colIndex < 0)
					throw new InvalidOperationException(String.Format("Cannot find column '{0}' in table '{1}'", columnName, table.FullName));

				columnInfos.Add(table.TableInfo[colIndex]);
			}

			var assignments = new List<SqlAssignExpression[]>();

			foreach (var valueSet in values) {
				var valueAssign = new SqlAssignExpression[valueSet.Length];

				for (int i = 0; i < valueSet.Length; i++) {
					var columnInfo = columnInfos[i];

					var value = valueSet[i];
					if (value != null) {
						// TODO: Deference columns with a preparer
					}

					if (value != null) {
						var expReturnType = value.ReturnType(context, null);
						if (!columnInfo.ColumnType.IsComparable (expReturnType))
						{
							var sb = new StringBuilder ();
							sb.AppendFormat ("Unable to convert type {0} of {1} into type {2} of column {3}", expReturnType, value, columnInfo.ColumnType, columnInfo.FullColumnName.FullName);
							var ioe = new InvalidOperationException (sb.ToString());
							throw ioe;
						}
					}

					valueAssign[i] = SqlExpression.Assign(SqlExpression.Reference(columnInfo.FullColumnName), value);
				}

				assignments.Add(valueAssign);
			}

			return new Prepared(tableName, assignments);
		}

		private ObjectName ResolveColumn(IFromTableSource fromTable, ObjectName v) {
			// Try and resolve against alias names first,
			var list = new List<ObjectName>();

			var tname = v.Parent;
			string schemaName = null;
			string tableName = null;
			string columnName = v.Name;
			if (tname != null) {
				schemaName = tname.ParentName;
				tableName = tname.Name;
			}

			int rcc = fromTable.ResolveColumnCount(null, schemaName, tableName, columnName);
			if (rcc == 1) {
				var matched = fromTable.ResolveColumn(null, schemaName, tableName, columnName);
				list.Add(matched);
			} else if (rcc > 1) {
				throw new StatementException("Ambiguous column name (" + v + ")");
			}

			int totalMatches = list.Count;
			if (totalMatches == 0)
				throw new StatementException("Can't find column: " + v);

			if (totalMatches == 1)
				return list[0];

			if (totalMatches > 1)
				// if there more than one match, check if they all match the identical
				// resource,
				throw new StatementException("Ambiguous column name (" + v + ")");

			// Should never reach here but we include this exception to keep the
			// compiler happy.
			throw new InvalidOperationException("Negative total matches");
		}


		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {
			internal Prepared(ObjectName tableName, IList<SqlAssignExpression[]> assignments) {
				TableName = tableName;
				Assignments = assignments;
			}

			private Prepared(SerializationInfo info, StreamingContext context) {
				TableName = (ObjectName) info.GetValue("TableName", typeof (ObjectName));
				int setCount = info.GetInt32("SetCount");
				var assignmenets = new SqlAssignExpression[setCount][];
				for (int i = 0; i < setCount; i++) {
					assignmenets[i] =
						(SqlAssignExpression[]) info.GetValue(String.Format("Assign[{0}]", i), typeof (SqlAssignExpression[]));
				}

				Assignments = assignmenets;
			}

			public ObjectName TableName { get; private set; }

			public IList<SqlAssignExpression[]> Assignments { get; private set; }

			protected override void GetData(SerializationInfo info) {
				info.AddValue("TableName", TableName);

				int setCount = Assignments.Count;
				info.AddValue("SetCount", setCount);

				for (int i = 0; i < setCount; i++) {
					var set = Assignments[i];
					info.AddValue(String.Format("Assign[{0}]", i), set);
				}
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				int insertCount = 0;

				if (!context.Query.Session.Access.UserCanInsertIntoTable(TableName))
					throw new MissingPrivilegesException(context.Query.UserName(), TableName, Privileges.Insert);

				var table = context.Query.Access.GetMutableTable(TableName);
				if (table == null)
					throw new ObjectNotFoundException(TableName);

				foreach (var assignment in Assignments) {
					var columnNames =
						assignment.Select(x => x.ReferenceExpression)
							.Cast<SqlReferenceExpression>()
							.Select(x => x.ReferenceName.Name).ToArray();

					var row = table.NewRow();

					foreach (var expression in assignment) {
						row.EvaluateAssignment(expression, context.Request);
					}

					table.AddRow(row);
					insertCount++;
				}

				context.SetResult(insertCount);
			}
		}

		#endregion

		#region PreparedSerializer

		//internal class PreparedSerializer : ObjectBinarySerializer<Prepared> {
		//	public override void Serialize(Prepared obj, BinaryWriter writer) {
		//		ObjectName.Serialize(obj.TableName, writer);

		//		var setListCount = obj.Assignments.Count;
		//		writer.Write(setListCount);

		//		for (int i = 0; i < setListCount; i++) {
		//			var set = obj.Assignments[i];
		//			var setCount = set.Length;

		//			writer.Write(setCount);

		//			for (int j = 0; j < setCount; j++) {
		//				SqlExpression.Serialize(obj.Assignments[i][j], writer);
		//			}
		//		}
		//	}

		//	public override Prepared Deserialize(BinaryReader reader) {
		//		var tableName = ObjectName.Deserialize(reader);

		//		var listCount = reader.ReadInt32();

		//		var setList = new List<SqlAssignExpression[]>(listCount);

		//		for (int i = 0; i < listCount; i++) {
		//			var setCount = reader.ReadInt32();

		//			var exps = new SqlAssignExpression[setCount];

		//			for (int j = 0; j < setCount; j++) {
		//				exps[j] = (SqlAssignExpression) SqlExpression.Deserialize(reader);
		//			}

		//			setList.Add(exps);
		//		}

		//		return new Prepared(tableName, setList);
		//	}
		//}

		#endregion
	}
}
