using System;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class CreateSchemaStringFormatTests {
		[Test]
		public static void SimpleCreate() {
			var statement = new CreateSchemaStatement("test");

			var sql = statement.ToString();
			var expected = "CREATE SCHEMA test";

			Assert.AreEqual(expected, sql);
		}
	}
}
