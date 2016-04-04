using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropViewTests : ContextBasedTest {
		protected override void OnSetUp(string testName, IQuery query) {
			CreateTestView(query);
		}

		private static void CreateTestView(IQuery query) {
			var tn1 = ObjectName.Parse("APP.test_table1");
			var tableInfo1 = new TableInfo(tn1);
			tableInfo1.AddColumn(new ColumnInfo("id", PrimitiveTypes.Integer()));
			tableInfo1.AddColumn(new ColumnInfo("name", PrimitiveTypes.String()));
			tableInfo1.AddColumn(new ColumnInfo("date", PrimitiveTypes.DateTime()));
			query.Session.Access().CreateTable(tableInfo1);
			query.Session.Access().AddPrimaryKey(tn1, "id");

			var exp = SqlExpression.Parse("SELECT * FROM APP.test_table1");
			query.CreateView(ObjectName.Parse("APP.test_view1"), (SqlQueryExpression)exp);
		}

		protected override void OnTearDown(string testName, IQuery query) {
			var tn1 = ObjectName.Parse("APP.test_table1");
			var viewName = ObjectName.Parse("APP.test_view1");

			query.Access().DropAllTableConstraints(tn1);
			query.Access().DropObject(DbObjectType.View, viewName);
			query.Access().DropObject(DbObjectType.Table, tn1);
		}

		[Test]
		public void DropView() {
			var viewName = ObjectName.Parse("APP.test_view1");

			Query.DropView(viewName);

			var exists = Query.Session.Access().ViewExists(viewName);
			Assert.IsFalse(exists);
		}

		[Test]
		public void IfExists_Existing() {
			var viewName = ObjectName.Parse("APP.test_view1");

			Query.DropView(viewName, true);

			var exists = Query.Session.Access().ViewExists(viewName);
			Assert.IsFalse(exists);
		}

		[Test]
		public void IfExists_NotExisting() {
			var viewName = ObjectName.Parse("APP.test_view2");

			Query.DropView(viewName, true);

			var exists = Query.Session.Access().ViewExists(viewName);
			Assert.IsFalse(exists);
		}

	}
}
