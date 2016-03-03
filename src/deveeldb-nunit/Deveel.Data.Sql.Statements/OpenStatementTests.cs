using System;

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public sealed class OpenStatementTests : ContextBasedTest {
		protected override IQuery CreateQuery(ISession session) {
			var query = base.CreateQuery(session);
			CreateTable(query);
			DeclareCursors(query);
			return query;
		}

		private void CreateTable(IQuery query) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String(), false);
			query.CreateTable(tableInfo, false, false);
		}

		private void DeclareCursors(IQuery query) {
			var queryExp1 = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM APP.test_table");
			query.DeclareCursor("c1", queryExp1);


			var queryExp2 = (SqlQueryExpression) SqlExpression.Parse("SELECT * FROM APP.test_table WHERE a > :a");
			var cursorInfo = new CursorInfo("c2", queryExp2);
			cursorInfo.Parameters.Add(new CursorParameter("a", PrimitiveTypes.Integer()));
			query.DeclareCursor(cursorInfo);
		}

		[Test]
		public void OpenSimple() {
			var statement = new OpenStatement("c1");

			statement.Execute(Query);

			var cursor = Query.FindCursor("c1");
			Assert.IsNotNull(cursor);
			Assert.AreEqual("c1", cursor.CursorInfo.CursorName);
			Assert.AreEqual(CursorStatus.Open, cursor.Status);
		}

		// TODO: Open a cursor should create a context in which to inject the arguments
		//[Test]
		//public void OpenWithArguments() {
		//	var args = new SqlExpression[] {SqlExpression.Constant(Field.Integer(22))};
		//	var statement = new OpenStatement("c2", args);

		//	statement.Execute(Query);

		//	var cursor = Query.FindCursor("c2");
		//	Assert.IsNotNull(cursor);
		//	Assert.AreEqual("c2", cursor.CursorInfo.CursorName);
		//	Assert.AreEqual(CursorStatus.Open, cursor.Status);
		//}
	}
}
