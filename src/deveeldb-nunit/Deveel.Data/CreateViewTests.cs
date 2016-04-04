using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CreateViewTests : ContextBasedTest {
		protected override void OnSetUp(string testName, IQuery query) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String(), false);

			query.Access().CreateTable(tableInfo);
		}

		[Test]
		public void SimpleView() {
			var query = (SqlQueryExpression) SqlExpression.Parse("SELECT * FROM test_table WHERE a = 1");
			var viewName = ObjectName.Parse("APP.text_view1");

			Query.CreateView(viewName, query);

			// TODO: Assert the view exists
		}
	}
}
