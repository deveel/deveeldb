using System;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class DropStringFormatTests {
		[Test]
		public static void DropType() {
			var statement = new DropTypeStatement(ObjectName.Parse("SYS.type1"));

			var sql = statement.ToString();
			var expected = "DROP TYPE SYS.type1";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void DropType_IfExists() {
			var statement = new DropTypeStatement(ObjectName.Parse("SYS.type1"), true);

			var sql = statement.ToString();
			var expected = "DROP TYPE IF EXISTS SYS.type1";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void DropTable() {
			var statement = new DropTableStatement(ObjectName.Parse("APP.test_table"));

			var sql = statement.ToString();
			var expected = "DROP TABLE APP.test_table";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void DropTable_IfExists() {
			var statement = new DropTableStatement(ObjectName.Parse("APP.test_table"), true);

			var sql = statement.ToString();
			var expected = "DROP TABLE IF EXISTS APP.test_table";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void DropFunction() {
			var statement = new DropFunctionStatement(ObjectName.Parse("APP.func1"));

			var sql = statement.ToString();
			var expected = "DROP FUNCTION APP.func1";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void DropFunction_IfExists() {
			var statement = new DropFunctionStatement(ObjectName.Parse("APP.func1"), true);

			var sql = statement.ToString();
			var expected = "DROP FUNCTION IF EXISTS APP.func1";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void DropProcedure() {
			var statement = new DropProcedureStatement(ObjectName.Parse("proc2"));

			var sql = statement.ToString();
			var expected = "DROP PROCEDURE proc2";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void DropProcedure_IfExists() {
			var statement = new DropProcedureStatement(ObjectName.Parse("proc2"), true);

			var sql = statement.ToString();
			var expected = "DROP PROCEDURE IF EXISTS proc2";

			Assert.AreEqual(expected, sql);
		}
	}
}
