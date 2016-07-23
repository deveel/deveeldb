using System;
using System.Text;

using Deveel.Data.Sql.Expressions;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class PlSqlBlockStringFormatTests {
		[Test]
		public static void CodeBlock_Nolabel() {
			var block = new PlSqlBlockStatement();
			block.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("a"), SqlExpression.Constant(2)));

			var sql = block.ToString();
			var expected = new StringBuilder();
			expected.AppendLine("BEGIN");
			expected.AppendLine("  :a := 2");
			expected.Append("END");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void CodeBlock_WithLabel() {
			var block = new PlSqlBlockStatement();
			block.Label = "stmt";
			block.Statements.Add(new CallStatement(ObjectName.Parse("proc1"), new[] {
				SqlExpression.Constant(33)
			}));

			var sql = block.ToString();
			var expected = new StringBuilder();
			expected.AppendLine("<<stmt>>");
			expected.AppendLine("BEGIN");
			expected.AppendLine("  CALL proc1(33)");
			expected.Append("END");

			Assert.AreEqual(expected.ToString(), sql);
		}
	}
}
