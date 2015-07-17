using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class AlterCreateTableStatement : SqlStatement {
		public override StatementType StatementType {
			get { return StatementType.AlterTable; }
		}

		public string TableName { get; set; }

		public CreateTableStatement CreateStatement { get; set; }

		protected override SqlPreparedStatement PrepareStatement(IExpressionPreparer preparer, IQueryContext context) {
			if (String.IsNullOrEmpty(TableName))
				throw new StatementPrepareException("The table name must be specified.");

			if (CreateStatement == null)
				throw new StatementPrepareException("The CREATE TABLE statement is required.");

			var tableName = context.ResolveTableName(TableName);
			var preparedCreate = CreateStatement.Prepare(preparer, context);

			return new PreparedAlterTableStatement(tableName, preparedCreate);
		}

		#region PreparedAlterTableStatement

		class PreparedAlterTableStatement : SqlPreparedStatement {
			public PreparedAlterTableStatement(ObjectName tableName, SqlPreparedStatement createStatement) {
				TableName = tableName;
				CreateStatement = createStatement;
			}

			public ObjectName TableName { get; private set; }

			public SqlPreparedStatement CreateStatement { get; private set; }

			public override ITable Evaluate(IQueryContext context) {
				throw new NotImplementedException();
			}
		}

		#endregion
	}
}
