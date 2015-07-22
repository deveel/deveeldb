using System;

using NUnit.Framework;

namespace Deveel.Data.Sql.Parser {
	[TestFixture]
	public class SqlParseTests {
		[Test]
		public void InsertIntoSyntaxError() {
			const string sql = "INSERT IN table VALUES (1, 'name');";

			SqlParseResult result = null;
			Assert.DoesNotThrow(() => result = SqlParsers.Default.Parse(sql));
			Assert.IsNotNull(result);
			Assert.IsNotEmpty(result.Errors);
		}
	}
}
