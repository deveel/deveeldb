using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class DropSequenceTests : SqlCompileTestBase {
		[Test]
		public void DropOneSequence() {
			const string sql = "DROP SEQUENCE seq1";

			var result = Compile(sql);
			
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DropSequenceStatement>(statement);

			var drop = (DropSequenceStatement) statement;

			Assert.AreEqual("seq1", drop.SequenceName.FullName);
		}

		[Test]
		public void DropTwoSequences() {
			const string sql = "DROP SEQUENCE seq1, APP.seq2";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(2, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DropSequenceStatement>(statement);

			var drop = (DropSequenceStatement)statement;

			Assert.AreEqual("seq1", drop.SequenceName.FullName);
		}
	}
}
