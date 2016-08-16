using System;
using System.Text;

using Deveel.Data.Sql.Expressions;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class LoopStringFormatTests {
		[Test]
		public static void Loop_NoLabel() {
			var loop = new LoopStatement();
			loop.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("a"), SqlExpression.Constant("two")));
			loop.Statements.Add(new ConditionStatement(SqlExpression.Constant(true), new SqlStatement[] {
				new ExitStatement()
			}));

			var sql = loop.ToString();
			var expected = new StringBuilder();
			expected.AppendLine("LOOP");
			expected.AppendLine("  :a := 'two'");
			expected.AppendLine("  IF TRUE THEN");
			expected.AppendLine("    EXIT");
			expected.AppendLine("  END IF");
			expected.Append("END LOOP");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void WhileLoop() {
			var loop = new WhileLoopStatement(SqlExpression.Constant(true));
			loop.Statements.Add(new CallStatement(ObjectName.Parse("SYSTEM.print"), new SqlExpression[] {
				SqlExpression.Constant("iterated")
			}));

			var sql = loop.ToString();
			var expected = new StringBuilder();
			expected.AppendLine("WHILE TRUE");
			expected.AppendLine("LOOP");
			expected.AppendLine("  CALL SYSTEM.print('iterated')");
			expected.Append("END LOOP");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void ForLoopGoto() {
			var loop = new ForLoopStatement("i", SqlExpression.Constant(0), SqlExpression.Reference(new ObjectName("x")));
			loop.Statements.Add(new ConditionStatement(SqlExpression.Equal(SqlExpression.VariableReference("i"), SqlExpression.Constant(3)), new SqlStatement[] {
				new GoToStatement("lbl_3") 
			}));

			var sql = loop.ToString();
			var expected = new StringBuilder();
			expected.AppendLine("FOR i IN 0...x");
			expected.AppendLine("LOOP");
			expected.AppendLine("  IF :i = 3 THEN");
			expected.AppendLine("    GOTO 'lbl_3'");
			expected.AppendLine("  END IF");
			expected.Append("END LOOP");

			Assert.AreEqual(expected.ToString(), sql);
		}
	}
}
