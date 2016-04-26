using System;
using System.Text;

using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class SqlStringFormatTests {
		[Test]
		public static void CreateTable() {
			var statement = new CreateTableStatement(new ObjectName("test_table"),
				new[] {
					new SqlTableColumn("a", PrimitiveTypes.Integer()),
					new SqlTableColumn("b", PrimitiveTypes.String()), 
				});

			var sql = statement.ToString();

			var expected = new StringBuilder();
			expected.AppendLine("CREATE TABLE test_table (");
			expected.AppendLine("  a INTEGER,");
			expected.AppendLine("  b STRING");
			expected.Append(")");

			Assert.AreEqual(expected.ToString(), sql);
		}
	}
}
