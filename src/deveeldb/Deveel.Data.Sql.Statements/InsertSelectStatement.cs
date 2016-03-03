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

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public sealed class InsertSelectStatement : SqlStatement, IPreparableStatement {
		public InsertSelectStatement(ObjectName tableName, SqlQueryExpression queryExpression) 
			: this(tableName, null, queryExpression) {
		}

		public InsertSelectStatement(ObjectName tableName, IEnumerable<string> columnNames, SqlQueryExpression queryExpression) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (queryExpression == null)
				throw new ArgumentNullException("queryExpression");

			TableName = tableName;
			ColumnNames = columnNames;
			QueryExpression = queryExpression;
		}

		public ObjectName	 TableName { get; private set; }

		public IEnumerable<string> ColumnNames { get; private set; }

		public SqlQueryExpression QueryExpression { get; private set; }

		IStatement IPreparableStatement.Prepare(IRequest request) {
			var tableName = request.Query.ResolveTableName(TableName);
			if (tableName == null)
				throw new ObjectNotFoundException(TableName);

			var columns = new string[0];
			if (ColumnNames != null)
				columns = ColumnNames.ToArray();

			ITableQueryInfo tableQueryInfo = request.Query.GetTableQueryInfo(tableName, null);
			var fromTable = new FromTableDirectSource(request.Query.IgnoreIdentifiersCase(), tableQueryInfo, "INSERT_TABLE", tableName, tableName);

			// Get the table we are inserting to
			var insertTable = request.Query.GetTable(tableName);

			if (columns.Length == 0) {
				columns = new string[insertTable.TableInfo.ColumnCount];
				for (int i = 0; i < columns.Length; i++) {
					columns[i] = insertTable.TableInfo[i].ColumnName;
				}
			}

			var colIndices = new int[columns.Length];
			var colResolved = new ObjectName[columns.Length];
			for (int i = 0; i < columns.Length; ++i) {
				var inVar = new ObjectName(columns[i]);
				var col = ResolveColumn(fromTable, inVar);
				int index = insertTable.FindColumn(col);
				if (index == -1)
					throw new InvalidOperationException(String.Format("Cannot find column '{0}' in table '{1}'.", col, tableName));

				colIndices[i] = index;
				colResolved[i] = col;
			}


			var queryPlan = request.Context.QueryPlanner().PlanQuery(new QueryInfo(request, QueryExpression));
			return new Prepared(tableName, colResolved, colIndices, queryPlan);
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
			internal Prepared(ObjectName tableName, ObjectName[] columnNames, int[] columnIndices, IQueryPlanNode queryPlan) {
				TableName = tableName;
				ColumnNames = columnNames;
				QueryPlan = queryPlan;
				ColumnIndices = columnIndices;
			}

			private Prepared(ObjectData data) {
				TableName = data.GetValue<ObjectName>("TableName");
				QueryPlan = data.GetValue<IQueryPlanNode>("QueryPlan");
				ColumnNames = data.GetValue<ObjectName[]>("ColumnNames");
				ColumnIndices = data.GetValue<int[]>("ColumnIndices");
			}

			public ObjectName TableName { get; private set; }

			public int[] ColumnIndices { get; private set; }

			public IQueryPlanNode QueryPlan { get; private set; }

			public ObjectName[] ColumnNames { get; private set; }

			protected override void GetData(SerializeData data) {
				data.SetValue("TableName", TableName);
				data.SetValue("QueryPlan", QueryPlan);
				data.SetValue("ColumnNames", ColumnNames);
				data.SetValue("ColumnIndices", ColumnIndices);
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				var insertTable = context.Request.Query.GetMutableTable(TableName);

				if (insertTable == null)
					throw new ObjectNotFoundException(TableName);

				var insertCount = 0;

				// Insert rows from the result select table.
				var result = QueryPlan.Evaluate(context.Request);
				if (result.ColumnCount() != ColumnIndices.Length) {
					throw new InvalidOperationException("Number of columns in result does not match columns to insert.");
				}

				foreach (var insertRow in result) {
					var newRow = insertTable.NewRow();

					for (int n = 0; n < ColumnIndices.Length; ++n) {
						var cell = insertRow.GetValue(n);
						newRow.SetValue(ColumnIndices[n], cell);
					}

					newRow.SetDefault(context.Request.Query);

					insertTable.AddRow(newRow);
					++insertCount;

				}

				context.SetResult(insertCount);
			}
		}

		#endregion
	}
}
