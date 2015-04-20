using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public class CreateViewStatementTests {
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
	}
}
