using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public class CreateViewStatementTests : ContextBasedTest {

		protected override IQueryContext CreateQueryContext(IDatabase database) {
			// We first create the table in another context...
			using (var session = database.CreateUserSession(AdminUserName, AdminPassword)) {
				using (var context = new SessionQueryContext(session)) {
					var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
					tableInfo.AddColumn("a", PrimitiveTypes.Integer());
					tableInfo.AddColumn("b", PrimitiveTypes.String(), false);

					context.CreateTable(tableInfo, false, false);
				}

				session.Commit();
			}

			return base.CreateQueryContext(database);
		}

		[Test]
		public void ParseSimpleCreateView() {
			const string sql = "CREATE VIEW text_view1 AS SELECT * FROM test_table WHERE a = 1";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var statementList = statements.ToList();
			Assert.IsNotEmpty(statementList);
			Assert.AreEqual(1, statementList.Count);
			Assert.IsInstanceOf<CreateViewStatement>(statementList[0]);

			var createView = (CreateViewStatement) statementList[0];
			Assert.IsNotNull(createView.SourceQuery);
			Assert.IsTrue(createView.IsFromQuery);

			Assert.IsNotNull(createView.ViewName);
		}


		[Test]
		public void ParseCreateViewWithColumns() {
			const string sql = "CREATE VIEW text_view1 (a, b, c) AS SELECT * FROM test_table WHERE a = 1";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var statementList = statements.ToList();
			Assert.IsNotEmpty(statementList);
			Assert.AreEqual(1, statementList.Count);
			Assert.IsInstanceOf<CreateViewStatement>(statementList[0]);

			var createView = (CreateViewStatement)statementList[0];
		}

		[Test]
		public void ParseCreateViewWithOrReplace() {
			const string sql = "CREATE OR REPLACE VIEW text_view1 AS SELECT * FROM test_table WHERE a = 1";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var statementList = statements.ToList();
			Assert.IsNotEmpty(statementList);
			Assert.AreEqual(1, statementList.Count);
			Assert.IsInstanceOf<CreateViewStatement>(statementList[0]);

			var createView = (CreateViewStatement)statementList[0];
		}

		[Test]
		public void ExecuteSimpleCreateView() {
			const string sql = "CREATE VIEW text_view1 AS SELECT * FROM test_table WHERE a = 1";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var list = statements.ToList();

			Assert.AreEqual(1, list.Count);

			var statement = list[0];

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateViewStatement>(statement);

			ITable result = null;
			Assert.DoesNotThrow(() => result = statement.Evaluate(QueryContext));
			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
		}
	}
}
