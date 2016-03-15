using System;

using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public sealed class GrantStatementTests : ContextBasedTest {
		protected override IQuery CreateQuery(ISession session) {
			var query = base.CreateQuery(session);
			query.Session.Access.CreateUser("test_user", "12345");

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
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			query.Session.Access.CreateTable(tableInfo);
			query.Session.Access.AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");

			tableInfo = new TableInfo(ObjectName.Parse("APP.test_table2"));
			tableInfo.AddColumn("person_id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("value", PrimitiveTypes.Boolean());

			query.Session.Access.CreateTable(tableInfo);
		}



		[Test]
		public void GrantCreateToUserOnSchema() {
			var statement = new GrantPrivilegesStatement("test_user", Privileges.Create, new ObjectName("APP"));

			var result = Query.ExecuteStatement(statement);

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
			Assert.AreEqual(1, result.TableInfo.ColumnCount);
			Assert.AreEqual(0, ((SqlNumber)result.GetValue(0, 0).Value).ToInt32());

			// TODO: Query the user privileges
		}

		[Test]
		public void GrantSelectToUserOnTable() {
			var statement = new GrantPrivilegesStatement("test_user", Privileges.Select, ObjectName.Parse("APP.test_table"));

			var result = Query.ExecuteStatement(statement);

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
			Assert.AreEqual(1, result.TableInfo.ColumnCount);
			Assert.AreEqual(0, ((SqlNumber)result.GetValue(0, 0).Value).ToInt32());

			// TODO: Query the user privileges
		}

		[Test]
		public void GrantSelectUpdateToUserOnTable() {
			
		}
	}
}
