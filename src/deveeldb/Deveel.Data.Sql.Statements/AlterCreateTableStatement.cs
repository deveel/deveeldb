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

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class AlterCreateTableStatement : SqlStatement {
		public AlterCreateTableStatement(string tableName, CreateTableStatement createStatement) {
			TableName = tableName;
			CreateStatement = createStatement;
		}

		public string TableName { get; private set; }

		public CreateTableStatement CreateStatement { get; private set; }

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
