using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Views;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public sealed class DropViewStatementTests : ContextBasedTest {
		protected override IQuery CreateQuery(ISession session) {
			var query = base.CreateQuery(session);
			CreateTestView(query);
			return query;
		}

		private void CreateTestView(IQuery query) {
			var tn1 = ObjectName.Parse("APP.test_table1");
			var tableInfo1 = new TableInfo(tn1);
			tableInfo1.AddColumn(new ColumnInfo("id", PrimitiveTypes.Integer()));
			tableInfo1.AddColumn(new ColumnInfo("name", PrimitiveTypes.String()));
			tableInfo1.AddColumn(new ColumnInfo("date", PrimitiveTypes.DateTime()));
			query.CreateTable(tableInfo1);
			query.AddPrimaryKey(tn1, "id");

			var exp = SqlExpression.Parse("SELECT * FROM APP.test_table1");
			query.ExecuteCreateView("APP.test_view1", (SqlQueryExpression) exp);
		}

		[Test]
		public void DropView() {
			var viewName = ObjectName.Parse("APP.test_view1");
			var statement = new DropViewStatement(viewName);

			statement.Execute(Query);

			var exists = Query.ViewExists(viewName);
			Assert.IsFalse(exists);
		}

		[Test]
		public void DropViewIfExists_Existing() {
			var viewName = ObjectName.Parse("APP.test_view1");
			var statement = new DropViewStatement(viewName, true);

			statement.Execute(Query);

			var exists = Query.ViewExists(viewName);
			Assert.IsFalse(exists);
		}

		[Test]
		public void DropViewIfExists_NotExisting() {
			var viewName = ObjectName.Parse("APP.test_view2");
			var statement = new DropViewStatement(viewName, true);

			statement.Execute(Query);

			var exists = Query.ViewExists(viewName);
			Assert.IsFalse(exists);
		}

	}
}
