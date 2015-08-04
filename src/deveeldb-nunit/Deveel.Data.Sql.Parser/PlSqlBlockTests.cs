using System;

using NUnit.Framework;

namespace Deveel.Data.Sql.Parser {
	[TestFixture]
	public static class PlSqlBlockTests {
		[Test]
		public static void ParseCreateSimpleTrigger() {
			const string sql = @"CREATE OR REPLACE TRIGGER test_trigger BEFORE INSERT ON test_table FOR EACH ROW
									DECLARE
										a BOOLEAN NOT NULL;
									BEGIN
										SELECT INTO a FROM table2 WHERE b = 22;
									END";

			SqlParseResult result = null;
			Assert.DoesNotThrow(() => result = SqlParsers.Default.Parse(sql));
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
		}
	}
}
