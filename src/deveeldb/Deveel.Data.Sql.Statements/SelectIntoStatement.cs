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
using System.IO;
using System.Runtime.Serialization;

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public sealed class SelectIntoStatement : SqlStatement, IPreparableStatement {
		public SelectIntoStatement(SqlQueryExpression queryExpression, SqlExpression reference) {
			if (queryExpression == null)
				throw new ArgumentNullException("queryExpression");
			if (reference == null)
				throw new ArgumentNullException("reference");

			QueryExpression = queryExpression;
			Reference = reference;
		}

		public SqlQueryExpression QueryExpression { get; private set; }

		public SqlExpression Reference { get; private set; }

		public bool IsVariableReference {
			get { return Reference.ExpressionType == SqlExpressionType.VariableReference; }
		}

		public bool IsObjectReference {
			get { return Reference.ExpressionType == SqlExpressionType.Reference; }
		}

		IStatement IPreparableStatement.Prepare(IRequest context) {
			var queryPlan = context.Query.Context.QueryPlanner().PlanQuery(new QueryInfo(context, QueryExpression));

			if (IsObjectReference) {
				var tableRef = ((SqlReferenceExpression) Reference).ReferenceName;
				return new Prepared(queryPlan, tableRef);
			}

			if (IsVariableReference) {
				throw new NotImplementedException();
			}

			// Other (impossible) case...
			throw new NotSupportedException();
		}

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {
			internal Prepared(IQueryPlanNode queryPlan, ObjectName table)
				: this(queryPlan) {
				IsForTable = true;
				Table = table;
			}

			internal Prepared(IQueryPlanNode queryPlan, string varName)
				: this(queryPlan) {
				IsForTable = false;
				VariableName = varName;
			}

			private Prepared(IQueryPlanNode queryPlan) {
				QueryPlan = queryPlan;
			}

			private Prepared(SerializationInfo info, StreamingContext context) {
				IsForTable = info.GetBoolean("IsForTable");
				QueryPlan = (IQueryPlanNode) info.GetValue("QueryPlan", typeof(IQueryPlanNode));
				Table = (ObjectName) info.GetValue("Table", typeof(ObjectName));
				VariableName = info.GetString("VariableName");
			}

			public bool IsForTable { get; private set; }

			public IQueryPlanNode QueryPlan { get; private set; }

			public ObjectName Table { get; private set; }

			public string VariableName { get; private set; }

			protected override void GetData(SerializationInfo info, StreamingContext context) {
				info.AddValue("IsForTable", IsForTable);
				info.AddValue("QueryPlan", QueryPlan);
				info.AddValue("Table", Table);
				info.AddValue("VariableName", VariableName);
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				var result = QueryPlan.Evaluate(context.Request);

				if (IsForTable) {
					var table = context.Request.Query.GetMutableTable(Table);
					if (table == null)
						throw new StatementPrepareException(String.Format("Referenced table of the INTO statement '{0}' was not found or is not mutable.", Table));

					SelectIntoTable(table, result);
				}

				// TODO: get the variable from ref and check if the result is compatible and set it

				throw new NotImplementedException();
			}

			private void SelectIntoTable(IMutableTable table, ITable result) {
				if (!AreCompatible(table.TableInfo, result.TableInfo))
					throw new InvalidOperationException();


				for (int i = 0; i < result.RowCount; i++) {
					var newRow = table.NewRow();

					for (int j = 0; j < result.ColumnCount(); j++) {
						var value = result.GetValue(i, j);
						newRow.SetValue(j, value);
					}

					table.AddRow(newRow);
				}
			}

			private bool AreCompatible(TableInfo a, TableInfo b) {
				if (a.ColumnCount != b.ColumnCount)
					return false;

				for (int i = 0; i < a.ColumnCount; i++) {
					var aCol = a[i];
					var bCol = b[i];
					if (!aCol.ColumnType.IsComparable(bCol.ColumnType))
						return false;
				}

				return false;
			}
		}

		#endregion
	}
}
