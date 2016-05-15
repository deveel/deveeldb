using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class IfTests : SqlCompileTestBase {
		[Test]
		public void IfSimple() {
			const string sql = "IF :a = 25 THEN RETURN 34 END IF";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);
			Assert.IsInstanceOf<ConditionStatement>(result.Statements.ElementAt(0));

			var conditional = (ConditionStatement) result.Statements.ElementAt(0);

			Assert.IsNotNull(conditional);
			Assert.IsNotNull(conditional.ConditionExpression);
			Assert.IsNotNull(conditional.TrueStatements);
			Assert.IsNotEmpty(conditional.TrueStatements);
			Assert.IsEmpty(conditional.FalseStatements);
		}

		[Test]
		public void IfElsifSimple() {
			const string sql = "IF :a = 25 THEN RETURN 34 ELSIF :a = 45 THEN RETURN 'hey!' END IF";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);
			Assert.IsInstanceOf<ConditionStatement>(result.Statements.ElementAt(0));

			var conditional = (ConditionStatement)result.Statements.ElementAt(0);

			Assert.IsNotNull(conditional);
			Assert.IsNotNull(conditional.ConditionExpression);
			Assert.IsNotNull(conditional.TrueStatements);
			Assert.IsNotEmpty(conditional.TrueStatements);
			Assert.IsNotEmpty(conditional.FalseStatements);
		}

		[Test]
		public void IfElsifElseSimple() {
			const string sql = "IF :a = 25 THEN RETURN 34 ELSIF :a = 45 THEN RETURN 'hey!' ELSE RETURN 09 END IF";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);
			Assert.IsInstanceOf<ConditionStatement>(result.Statements.ElementAt(0));

			var conditional = (ConditionStatement)result.Statements.ElementAt(0);

			Assert.IsNotNull(conditional);
			Assert.IsNotNull(conditional.ConditionExpression);
			Assert.IsNotNull(conditional.TrueStatements);
			Assert.IsNotEmpty(conditional.TrueStatements);
			Assert.IsNotEmpty(conditional.FalseStatements);
		}
	}
}
