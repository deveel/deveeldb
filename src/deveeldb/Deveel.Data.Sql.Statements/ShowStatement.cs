using System;

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public sealed class ShowStatement : SqlStatement, IPreparableStatement {
		public ShowStatement(ShowTarget target) {
			Target = target;
		}

		public ShowTarget Target { get; private set; }

		public ObjectName TableName { get; set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			base.ExecuteStatement(context);
		}

		IStatement IPreparableStatement.Prepare(IRequest request) {
			ObjectName tableName = null;

			if (Target == ShowTarget.Table &&
			    TableName != null) {
				tableName = request.Query.ResolveTableName(TableName);
			}

			return new Prepared(Target, tableName);
		}

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {

			public Prepared(ShowTarget target, ObjectName tableName) {
				Target = target;
				TableName = tableName;
			}

			private Prepared(ObjectData data) {
				TableName = data.GetValue<ObjectName>("TableName");
				Target = (ShowTarget) data.GetInt32("Target");
			}

			public ObjectName TableName { get; private set; }

			public ShowTarget Target { get; private set; }

			protected override void GetData(SerializeData data) {
				data.SetValue("TableName", TableName);
				data.SetValue("Target", (int)Target);
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				base.ExecuteStatement(context);
			}
		}

		#endregion
	}
}
