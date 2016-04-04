using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DeclareCursorTests : ContextBasedTest {
		protected override void OnSetUp(string testName, IQuery query) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String(), false);

			query.Access().CreateTable(tableInfo);
		}

		protected override void OnTearDown(string testName, IQuery query) {
			query.Access().DropObject(DbObjectType.Table, ObjectName.Parse("APP.test_table"));
		}

		[Test]
		public void InsensitiveSimpleCursor() {
			const string cursorName = "c";
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM APP.test_table");

			Query.DeclareCursor(cursorName, query);

			var cursor = Query.Context.FindCursor(cursorName);
			Assert.IsNotNull(cursor);
			Assert.AreEqual(cursorName, cursor.CursorInfo.CursorName);
			Assert.IsEmpty(cursor.CursorInfo.Parameters);
		}

		[Test]
		public void InsensitiveWithParams() {
			const string cursorName = "c";
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM APP.test_table WHERE a = :a");
			var parameters = new[] { new CursorParameter("a", PrimitiveTypes.Integer()) };

			Query.DeclareCursor(cursorName, parameters, query);

			var cursor = Query.Context.FindCursor(cursorName);
			Assert.IsNotNull(cursor);
			Assert.AreEqual(cursorName, cursor.CursorInfo.CursorName);
			Assert.IsNotEmpty(cursor.CursorInfo.Parameters);
		}

	}
}
