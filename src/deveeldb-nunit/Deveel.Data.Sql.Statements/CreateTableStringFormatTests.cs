using System;
using System.Text;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class CreateTableStringFormatTests {
		[Test]
		public static void SimpleTable() {
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

		[Test]
		public static void WithIdentityColumn() {
			var statement = new CreateTableStatement(new ObjectName("test_table"),
				new[] {
					new SqlTableColumn("id", PrimitiveTypes.Integer()) {
						IsIdentity = true
					},
					new SqlTableColumn("name", PrimitiveTypes.String()),
				});

			var sql = statement.ToString();

			var expected = new StringBuilder();
			expected.AppendLine("CREATE TABLE test_table (");
			expected.AppendLine("  id INTEGER IDENTITY,");
			expected.AppendLine("  name STRING");
			expected.Append(")");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void IfNotExists() {
			var statement = new CreateTableStatement(new ObjectName("test_table"),
				new[] {
					new SqlTableColumn("a", PrimitiveTypes.Integer()),
					new SqlTableColumn("b", PrimitiveTypes.String()),
				}) {
					IfNotExists = true
				};

			var sql = statement.ToString();

			var expected = new StringBuilder();
			expected.AppendLine("CREATE TABLE IF NOT EXISTS test_table (");
			expected.AppendLine("  a INTEGER,");
			expected.AppendLine("  b STRING");
			expected.Append(")");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void ColumnWithDefault() {
			var statement = new CreateTableStatement(new ObjectName("test_table"),
				new[] {
					new SqlTableColumn("a", PrimitiveTypes.Integer()) {
						DefaultExpression = SqlExpression.Constant(1)
					},
					new SqlTableColumn("b", PrimitiveTypes.String()) {
						IsNotNull = true
					}
				});

			var sql = statement.ToString();

			var expected = new StringBuilder();
			expected.AppendLine("CREATE TABLE test_table (");
			expected.AppendLine("  a INTEGER DEFAULT 1,");
			expected.AppendLine("  b STRING NOT NULL");
			expected.Append(")");

			Assert.AreEqual(expected.ToString(), sql);
		}
	}
}
