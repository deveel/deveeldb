using System;
using System.Text;

using Deveel.Data.Sql.Expressions;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class InsertStringFormatTests {
		[Test]
		public static void WithColumnNames() {
			var statement = new InsertStatement(ObjectName.Parse("t1"), new [] {
				"first_name", "last_name", "age"
			}, new [] {
				new SqlExpression[] {
					SqlExpression.Constant("Antonello"),
					SqlExpression.Constant("Provenzano"),
					SqlExpression.Constant(34)
				}, 
			});

			var sql = statement.ToString();
			var expected = new StringBuilder();
			expected.Append("INSERT INTO t1(first_name, last_name, age) ");
			expected.Append("VALUES ('Antonello', 'Provenzano', 34)");
			
			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void WithNoColumns() {
			var statement = new InsertStatement(ObjectName.Parse("t1"), new[] {
				new SqlExpression[] {
					SqlExpression.Constant("Antonello"),
					SqlExpression.Constant("Provenzano"),
					SqlExpression.Constant(34)
				},
			});

			var sql = statement.ToString();
			var expected = new StringBuilder();
			expected.Append("INSERT INTO t1 ");
			expected.Append("VALUES ('Antonello', 'Provenzano', 34)");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void MultiSet() {
			var statement = new InsertStatement(ObjectName.Parse("t1"), new[] {
				"first_name", "last_name", "age"
			}, new[] {
				new SqlExpression[] {
					SqlExpression.Constant("Antonello"),
					SqlExpression.Constant("Provenzano"),
					SqlExpression.Constant(34)
				},
				new SqlExpression[] {
					SqlExpression.Constant("Sebastiano"),
					SqlExpression.Constant("Provenzano"),
					SqlExpression.Constant(33) 
				}, 
			});

			var sql = statement.ToString();
			var expected = new StringBuilder();
			expected.AppendLine("INSERT INTO t1(first_name, last_name, age)");
			expected.AppendLine("  VALUES");
			expected.AppendLine("  ('Antonello', 'Provenzano', 34),");
			expected.Append("  ('Sebastiano', 'Provenzano', 33)");

			Assert.AreEqual(expected.ToString(), sql);
		}
	}
}
