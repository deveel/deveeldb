using System;

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Statements {
	public sealed class DeleteStatement : SqlStatement, IPreparableStatement {
		public DeleteStatement(ObjectName tableName, SqlExpression whereExpression) 
			: this(tableName, whereExpression, -1) {
		}

		public DeleteStatement(ObjectName tableName, SqlExpression whereExpression, int limit) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (whereExpression == null)
				throw new ArgumentNullException("whereExpression");

			TableName = tableName;
			WhereExpression = whereExpression;
			Limit = limit;
		}

		public ObjectName TableName { get; private set; }

		public SqlExpression WhereExpression { get; private set; }

		public int Limit { get; set; }

		IStatement IPreparableStatement.Prepare(IRequest request) {
			throw new NotImplementedException();
		}

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {
			public Prepared(ObjectName tableName, IQueryPlanNode queryPlan) {
				TableName = tableName;
				QueryPlan = queryPlan;
			}

			private Prepared(ObjectData data)
				: base(data) {
				TableName = data.GetValue<ObjectName>("TableName");
				QueryPlan = data.GetValue<IQueryPlanNode>("QueryPlan");
			}

			public ObjectName TableName { get; private set; }

			public IQueryPlanNode QueryPlan { get; private set; }

			protected override void GetData(SerializeData data) {
				data.SetValue("TableName", TableName);
				data.SetValue("QueryPlan", QueryPlan);
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				base.ExecuteStatement(context);
			}
		}

		#endregion
	}
}
