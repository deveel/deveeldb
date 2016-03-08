using System;
using System.Linq;

using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public class DeclareVariableTests : SqlCompileTestBase {
		[Test]
		public void DeclareSimpleVariable() {
			const string sql = "DECLARE a NUMERIC(2, 3)";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);
			Assert.IsInstanceOf<DeclareVariableStatement>(result.Statements.First());

			var statement = (DeclareVariableStatement) result.Statements.First();
			Assert.AreEqual("a", statement.VariableName);
			Assert.AreEqual(false, statement.IsNotNull);
			Assert.AreEqual(false, statement.IsConstant);
			Assert.IsInstanceOf<NumericType>(statement.VariableType);
		}

		[Test]
		public void DeclareConstantVariable() {
			const string sql = "DECLARE a CONSTANT NUMERIC(2, 3)";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);
			Assert.IsInstanceOf<DeclareVariableStatement>(result.Statements.First());

			var statement = (DeclareVariableStatement)result.Statements.First();
			Assert.AreEqual("a", statement.VariableName);
			Assert.AreEqual(true, statement.IsNotNull);
			Assert.AreEqual(true, statement.IsConstant);
			Assert.IsInstanceOf<NumericType>(statement.VariableType);
		}

		[Test]
		public void ImplicitDeclaration() {
			const string sql = "a NUMERIC(2, 3)";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);
			Assert.IsInstanceOf<DeclareVariableStatement>(result.Statements.First());

			var statement = (DeclareVariableStatement)result.Statements.First();
			Assert.AreEqual("a", statement.VariableName);
			Assert.IsInstanceOf<NumericType>(statement.VariableType);
		}
	}
}
