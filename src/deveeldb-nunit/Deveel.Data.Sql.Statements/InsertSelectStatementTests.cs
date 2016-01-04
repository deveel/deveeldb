using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

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

			query.CreateTable(tableInfo);
			query.AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");
		}

		[Test]
		public void WithColumns() {
			var tableName = ObjectName.Parse("APP.test_table");
			var query = (SqlQueryExpression) SqlExpression.Parse("SELECT 'Antonello' AS first_name, 'Provenzano' AS last_name, NULL AS birth_date, NULL as active");
			var columns = new[] {"first_name", "last_name", "birth_date", "active"};

			var statement = new InsertSelectStatement(tableName, columns, query);

			Query.ExecuteStatement(statement);

			var table = Query.GetTable(tableName);

			Assert.AreEqual(1, table.RowCount);
		}

		[Test]
		public void WithNoColumns() {
			
		}
	}
}
