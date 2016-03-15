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
using System.Runtime.Serialization;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Views;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateViewStatement : SqlStatement {
		public CreateViewStatement(string viewName, SqlQueryExpression queryExpression) 
			: this(viewName, null, queryExpression) {
		}

		public CreateViewStatement(string viewName, IEnumerable<string> columnNames, SqlQueryExpression queryExpression) {
			if (viewName == null)
				throw new ArgumentNullException("viewName");
			if (queryExpression == null)
				throw new ArgumentNullException("queryExpression");

			ViewName = viewName;
			ColumnNames = columnNames;
			QueryExpression = queryExpression;
		}

		public string ViewName { get; private set; }

		public IEnumerable<string> ColumnNames { get; private set; }

		public SqlQueryExpression QueryExpression { get; private set; }

		public bool ReplaceIfExists { get; set; }

		protected override SqlStatement PrepareStatement(IRequest context) {
			var viewName = context.Query.Session.Access.ResolveTableName(ViewName);

			var queryFrom = QueryExpressionFrom.Create(context, QueryExpression);
			var queryPlan = context.Query.Context.QueryPlanner().PlanQuery(new QueryInfo(context, QueryExpression));

			var colList = ColumnNames == null ? new string[0] : ColumnNames.ToArray();

			// Wrap the result around a SubsetNode to alias the columns in the
			// table correctly for this view.
			int sz = colList.Length;
			var originalNames = queryFrom.GetResolvedColumns();
			var newColumnNames = new ObjectName[originalNames.Length];

			if (sz > 0) {
				if (sz != originalNames.Length)
					throw new InvalidOperationException("Column list is not the same size as the columns selected.");

				for (int i = 0; i < sz; ++i) {
					var colName = colList[i];
					newColumnNames[i] = new ObjectName(viewName, colName);
				}
			} else {
				sz = originalNames.Length;
				for (int i = 0; i < sz; ++i) {
					newColumnNames[i] = new ObjectName(viewName, originalNames[i].Name);
				}
			}

			// Check there are no repeat column names in the table.
			for (int i = 0; i < sz; ++i) {
				var columnName = newColumnNames[i];
				for (int n = i + 1; n < sz; ++n) {
					if (newColumnNames[n].Equals(columnName))
						throw new InvalidOperationException(String.Format("Duplicate column name '{0}' in view. A view may not contain duplicate column names.", columnName));
				}
			}

			// Wrap the plan around a SubsetNode plan
			queryPlan = new SubsetNode(queryPlan, originalNames, newColumnNames);

			return new Prepared(viewName, QueryExpression, queryPlan, ReplaceIfExists);
		}

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {
			internal Prepared(ObjectName viewName, SqlQueryExpression queryExpression, IQueryPlanNode queryPlan, bool replaceIfExists) {
				ViewName = viewName;
				QueryPlan = queryPlan;
				ReplaceIfExists = replaceIfExists;
				QueryExpression = queryExpression;
			}

			private Prepared(SerializationInfo info, StreamingContext context) {
				ViewName = (ObjectName) info.GetValue("Name", typeof(ObjectName));
				QueryPlan = (IQueryPlanNode) info.GetValue("QueryPlan", typeof(IQueryPlanNode));
				ReplaceIfExists = info.GetBoolean("ReplaceIfExists");
				QueryExpression = (SqlQueryExpression) info.GetValue("QueryExpression", typeof(SqlQueryExpression));
			}

			public ObjectName ViewName { get; private set; }

			public IQueryPlanNode QueryPlan { get; private set; }

			public bool ReplaceIfExists { get; set; }

			public SqlQueryExpression QueryExpression { get; private set; }

			protected override void GetData(SerializationInfo info) {
				info.AddValue("Name", ViewName);
				info.AddValue("QueryPlan", QueryPlan);
				info.AddValue("QueryExpression", QueryExpression);
				info.AddValue("ReplaceIfExists", ReplaceIfExists);
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				// We have to execute the plan to get the TableInfo that represents the
				// result of the view execution.
				var table = QueryPlan.Evaluate(context.Request);
				var tableInfo = table.TableInfo.Alias(ViewName);

				var viewInfo = new ViewInfo(tableInfo, QueryExpression, QueryPlan);
				context.Request.Query.Session.Access.DefineView(viewInfo, ReplaceIfExists);
			}
		}

		#endregion

		#region PreparedSerializer

		//internal class PreparedSerializer : ObjectBinarySerializer<Prepared> {
		//	public override void Serialize(Prepared obj, BinaryWriter writer) {
		//		ObjectName.Serialize(obj.ViewName, writer);

		//		var queryPlanTypeName = obj.QueryPlan.GetType().FullName;
		//		writer.Write(queryPlanTypeName);

		//		QueryPlanSerializers.Serialize(obj.QueryPlan, writer);
		//		SqlExpression.Serialize(obj.QueryExpression, writer);
		//		writer.Write(obj.ReplaceIfExists);
		//	}

		//	public override Prepared Deserialize(BinaryReader reader) {
		//		var viewName = ObjectName.Deserialize(reader);

		//		var queryPlanTypeName = reader.ReadString();
		//		var queryPlanType = Type.GetType(queryPlanTypeName, true);
		//		var queryPlan = QueryPlanSerializers.Deserialize(queryPlanType, reader);

		//		var queryExpression = SqlExpression.Deserialize(reader) as SqlQueryExpression;
		//		var replaceIfExists = reader.ReadBoolean();

		//		return new Prepared(viewName, queryExpression, queryPlan, replaceIfExists);
		//	}
		//}

		#endregion
	}
}
