using System;
using System.Text;

using Deveel.Data.Routines;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class CreateProcedureStringFormatTests {
		[Test]
		public static void WithNoParameters() {
			var body = new PlSqlBlockStatement();
			body.Declarations.Add(new DeclareVariableStatement("a", PrimitiveTypes.Integer()));
			body.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("a"), SqlExpression.Constant(3)));
			var statement = new CreateProcedureStatement(ObjectName.Parse("SYS.proc1"), body);

			var sql = statement.ToString();
			var expected = new SqlStringBuilder();
			expected.AppendLine("CREATE PROCEDURE SYS.proc1() IS");
			expected.AppendLine("  DECLARE");
			expected.AppendLine("    a INTEGER");
			expected.AppendLine("  BEGIN");
			expected.AppendLine("    :a := 3");
			expected.Append("  END");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void WithArguments() {
			var body = new PlSqlBlockStatement();
			body.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("a"), SqlExpression.Constant(3)));
			var statement = new CreateProcedureStatement(ObjectName.Parse("SYS.proc1"), new[] {
				new RoutineParameter("a", PrimitiveTypes.Integer()),
			}, body);

			var sql = statement.ToString();
			var expected = new SqlStringBuilder();
			expected.AppendLine("CREATE PROCEDURE SYS.proc1(a INTEGER IN NOT NULL) IS");
			expected.AppendLine("  BEGIN");
			expected.AppendLine("    :a := 3");
			expected.Append("  END");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void ExternalWithNoArguments() {
			var statement = new CreateExternalProcedureStatement(ObjectName.Parse("SYS.ext_proc2"), "Deveel.Data.ExtProcedures.Proc1()");

			var sql = statement.ToString();
			var expected = new StringBuilder();
			expected.AppendLine("CREATE EXTERNAL PROCEDURE SYS.ext_proc2() IS");
			expected.Append("  LANGUAGE DOTNET NAME 'Deveel.Data.ExtProcedures.Proc1()'");

			Assert.AreEqual(expected.ToString(), sql);
		}
	}
}
