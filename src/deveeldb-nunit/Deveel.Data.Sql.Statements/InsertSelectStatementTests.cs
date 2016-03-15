using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public class InsertSelectStatementTests : ContextBasedTest {
		protected override IQuery CreateQuery(ISession session) {
			var query = base.CreateQuery(session);
			CreateTestTable(query);
			return query;
		}

		private void CreateTestTable(IQuery query) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableInfo.TableName.FullName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime(), false);
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			query.Session.Access.CreateTable(tableInfo);
			query.Session.Access.AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");
		}

		[Test]
		public void WithColumns() {
			var tableName = ObjectName.Parse("APP.test_table");
			var query = (SqlQueryExpression) SqlExpression.Parse("SELECT 'Antonello', 'Provenzano', NULL, NULL");
			var columns = new[] {"first_name", "last_name", "birth_date", "active"};

			var statement = new InsertSelectStatement(tableName, columns, query);

			Query.ExecuteStatement(statement);

			var table = Query.Access.GetTable(tableName);

			Assert.AreEqual(1, table.RowCount);
		}

		[Test]
		public void WithNoColumns() {
			var tableName = ObjectName.Parse("APP.test_table");
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT 3, 'Antonello', 'Provenzano', NULL, NULL");

			var statement = new InsertSelectStatement(tableName, query);

			Query.ExecuteStatement(statement);

			var table = Query.Access.GetTable(tableName);

			Assert.AreEqual(1, table.RowCount);

		}
	}
}
