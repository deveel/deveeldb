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
using System.Runtime.Serialization;

using Deveel.Data.Security;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

namespace Deveel.Data.Sql.Statements {
	public sealed class SelectIntoStatement : SqlStatement, IPlSqlStatement {
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

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var reference = Reference.Prepare(preparer);
			var query = (SqlQueryExpression) QueryExpression.Prepare(preparer);

			return new SelectIntoStatement(query, reference);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var queryPlan = context.Query.Context.QueryPlanner().PlanQuery(new QueryInfo(context, QueryExpression));

			if (Reference is SqlReferenceExpression) {
				var objName = ((SqlReferenceExpression) Reference).ReferenceName;
				objName = context.Access().ResolveObjectName(objName);

				return new SelectIntoTable(objName, queryPlan);
			}

			if (Reference is SqlVariableReferenceExpression) {
				var refName = ((SqlVariableReferenceExpression) Reference).VariableName;
				return new SelectIntoVariable(new[] { refName}, queryPlan);
			}

			if (Reference is SqlTupleExpression) {
				var exps = ((SqlTupleExpression) Reference).Expressions;
				if (exps == null || exps.Length == 0)
					throw new StatementPrepareException("Empty tuple in SELECT INTO");

				var variables = new List<string>();

				for (int i = 0; i < exps.Length; i++) {
					if (!(exps[i] is SqlVariableReferenceExpression))
						throw new StatementPrepareException("Found an invalid expression in the tuple.");

					var varName = ((SqlVariableReferenceExpression) exps[i]).VariableName;
					variables.Add(varName);
				}

				return new SelectIntoVariable(variables.ToArray(), queryPlan);
			}

			// Other (impossible) case...
			throw new NotSupportedException();
		}

		private static int InsertIntoTable(IMutableTable table, ICursor cursor) {
			int addedd = 0;
			bool verified = false;
			foreach (var row in cursor) {
				if (!verified) {
					if (AreCompatible(table.TableInfo, cursor.Source.TableInfo))
						throw new StatementException("The source cursor is not compatible to the destination table.");

					verified = true;
				}

				var newRow = table.NewRow();
				for (int i = 0; i < row.ColumnCount; i++) {
					newRow.SetValue(i, row.GetValue(i));
				}

				table.AddRow(newRow);
				addedd++;
			}

			return addedd;
		}

		private static bool AreCompatible(TableInfo a, TableInfo b) {
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


		#region SelectIntoTable

		[Serializable]
		class SelectIntoTable : SqlStatement {
			public SelectIntoTable(ObjectName tableName, IQueryPlanNode queryPlan) {
				TableName = tableName;
				QueryPlan = queryPlan;
			}

			private SelectIntoTable(SerializationInfo info, StreamingContext context)
				: base(info, context) { 
				TableName = (ObjectName) info.GetValue("TableName", typeof(ObjectName));
				QueryPlan = (IQueryPlanNode)info.GetValue("QueryPlan", typeof(IQueryPlanNode));
			}

			public IQueryPlanNode QueryPlan { get; private set; }

			public ObjectName TableName { get; private set; }

			protected override void ExecuteStatement(ExecutionContext context) {
				if (!context.User.CanSelectFrom(QueryPlan))
					throw new SecurityException();

				var cursor = new NativeCursor(new NativeCursorInfo(QueryPlan), context.Request);

				var table = context.Request.Access().GetMutableTable(TableName);
				if (table == null)
					throw new StatementPrepareException(String.Format("Referenced table of the INTO statement '{0}' was not found or is not mutable.", TableName));

				var addedd = InsertIntoTable(table, cursor);

				context.SetResult(addedd);
			}

			protected override void GetData(SerializationInfo info) {
				info.AddValue("QueryPlan", QueryPlan);
				info.AddValue("TableName", TableName);
				base.GetData(info);
			}
		}

		#endregion

		#region SelectIntoVariable

		[Serializable]
		class SelectIntoVariable : SqlStatement {
			public SelectIntoVariable(string[] variableNames, IQueryPlanNode queryPlan) {
				VariableNames = variableNames;
				QueryPlan = queryPlan;
			}

			private SelectIntoVariable(SerializationInfo info, StreamingContext context)
				: base(info, context) {
				VariableNames = (string[]) info.GetValue("VariableName", typeof(string[]));
				QueryPlan = (IQueryPlanNode) info.GetValue("QueryPlan", typeof (IQueryPlanNode));
			}

			public string[] VariableNames { get; private set; }

			public IQueryPlanNode QueryPlan { get; private set; }

			protected override void ExecuteStatement(ExecutionContext context) {
				if (!context.User.CanSelectFrom(QueryPlan))
					throw new SecurityException();

				var cursor = new NativeCursor(new NativeCursorInfo(QueryPlan), context.Request);

				if (VariableNames.Length == 1) {
					var variable = context.Request.Context.FindVariable(VariableNames[0]);
					if (variable == null)
						throw new ObjectNotFoundException(new ObjectName(VariableNames[0]));

					if (variable.Type is TabularType) {
						var tabular = ((SqlTabular) variable.Value.Value);
						throw new NotImplementedException("Support insert into a tabular variable");
					}
				}

				var firstRow = cursor.FirstOrDefault();
				if (firstRow == null)
					// TODO: Is it correct to throw an error here?
					throw new StatementException("The query has returned no elements");

				if (firstRow.ColumnCount != VariableNames.Length)
					throw new StatementException("The selected number of items does not match with the number of destination variables.");

				for (int i = 0; i < VariableNames.Length; i++) {
					var variableName = VariableNames[i];
					var variable = context.Request.Context.FindVariable(variableName);
					if (variable == null)
						throw new ObjectNotFoundException(new ObjectName(variableName));

					var source = firstRow.GetValue(i);
					variable.SetValue(source);
				}

				context.SetResult(1);
			}

			protected override void GetData(SerializationInfo info) {
				info.AddValue("VariableNames", VariableNames);
				info.AddValue("QueryPlan", QueryPlan);
			}
		}

		#endregion
	}
}
