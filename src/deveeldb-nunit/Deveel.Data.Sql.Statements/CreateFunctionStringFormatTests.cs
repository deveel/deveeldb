using System;
using System.Text;

using Deveel.Data.Routines;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class CreateFunctionStringFormatTests {
		[Test]
		public static void WithNoParameters() {
			var body = new PlSqlBlockStatement();
			body.Declarations.Add(new DeclareVariableStatement("a", PrimitiveTypes.Integer()));
			body.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("a"), SqlExpression.Constant(3)));
			body.Statements.Add(new ReturnStatement(SqlExpression.VariableReference("a")));
			var statement = new CreateFunctionStatement(ObjectName.Parse("SYS.func1"), PrimitiveTypes.Integer(), body);

			var sql = statement.ToString();
			var expected = new SqlStringBuilder();
			expected.Append("CREATE FUNCTION SYS.func1() ");
			expected.AppendLine("RETURNS INTEGER IS");
			expected.AppendLine("  DECLARE");
			expected.AppendLine("    a INTEGER");
			expected.AppendLine("  BEGIN");
			expected.AppendLine("    :a := 3");
			expected.AppendLine("    RETURN :a");
			expected.Append("  END");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void WithArguments() {
			var body = new PlSqlBlockStatement();
			body.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("a"), SqlExpression.Constant(3)));
			body.Statements.Add(new ReturnStatement(SqlExpression.VariableReference("a")));
			var statement = new CreateFunctionStatement(ObjectName.Parse("SYS.func1"), PrimitiveTypes.Integer(), new[] {
				new RoutineParameter("a", PrimitiveTypes.Integer()), 
			}, body);

			var sql = statement.ToString();
			var expected = new SqlStringBuilder();
			expected.Append("CREATE FUNCTION SYS.func1(a INTEGER IN NOT NULL) ");
			expected.AppendLine("RETURNS INTEGER IS");
			expected.AppendLine("  BEGIN");
			expected.AppendLine("    :a := 3");
			expected.AppendLine("    RETURN :a");
			expected.Append("  END");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void ExternalWithNoArguments() {
			var statement = new CreateExternalFunctionStatement(ObjectName.Parse("SYS.ext_func2"), PrimitiveTypes.String(), "Deveel.Data.ExtFunctions.Func1()");

			var sql = statement.ToString();
			var expected = new StringBuilder();
			expected.Append("CREATE EXTERNAL FUNCTION SYS.ext_func2() ");
			expected.AppendLine("RETURNS STRING IS");
			expected.Append("  LANGUAGE DOTNET NAME 'Deveel.Data.ExtFunctions.Func1()'");

			Assert.AreEqual(expected.ToString(), sql);
		}
	}
}
