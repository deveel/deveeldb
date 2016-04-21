using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class CreateSequenceTets : SqlCompileTestBase {
		[Test]
		public void WithDefaultValues() {
			const string sql = "CREATE SEQUENCE seq1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);
			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateSequenceStatement>(statement);

			var createSequence = (CreateSequenceStatement) statement;
			Assert.IsNotNull(createSequence.SequenceName);
			Assert.AreEqual("seq1", createSequence.SequenceName.FullName);
			Assert.IsNull(createSequence.MinValue);
			Assert.IsNull(createSequence.MaxValue);
			Assert.IsNull(createSequence.Cache);
			Assert.IsNull(createSequence.IncrementBy);
			Assert.IsNull(createSequence.StartWith);
			Assert.IsFalse(createSequence.Cycle);
		}

		[Test]
		public void WithStart() {
			const string sql = "CREATE SEQUENCE seq1 START WITH 3 NOMINVALUE NOMAXVALUE";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);
			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateSequenceStatement>(statement);

			var createSequence = (CreateSequenceStatement)statement;
			Assert.IsNotNull(createSequence.SequenceName);
			Assert.AreEqual("seq1", createSequence.SequenceName.FullName);
			Assert.IsNotNull(createSequence.MinValue);
			Assert.IsNotNull(createSequence.MaxValue);
			Assert.IsNull(createSequence.Cache);
			Assert.IsNull(createSequence.IncrementBy);
			Assert.IsNotNull(createSequence.StartWith);
			Assert.IsFalse(createSequence.Cycle);
		}

		[Test]
		public void FullSpecified() {
			const string sql = "CREATE SEQUENCE seq1 START WITH 1 INCREMENT BY 1 MINVALUE 0 MAXVALUE 20000 CACHE 34 NOCYCLE";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);
			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateSequenceStatement>(statement);

			var createSequence = (CreateSequenceStatement)statement;
			Assert.IsNotNull(createSequence.SequenceName);
			Assert.AreEqual("seq1", createSequence.SequenceName.FullName);
			Assert.IsNotNull(createSequence.MinValue);
			Assert.IsNotNull(createSequence.MaxValue);
			Assert.IsNotNull(createSequence.Cache);
			Assert.IsNotNull(createSequence.IncrementBy);
			Assert.IsNotNull(createSequence.StartWith);
			Assert.IsFalse(createSequence.Cycle);
		}
	}
}
