using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class CreateTypeTests : SqlCompileTestBase {
		[Test]
		public void SimpleType() {
			const string sql = "CREATE OR REPLACE TYPE App.type1 AS OBJECT (year INT, name VARCHAR)";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateTypeStatement>(statement);
		}

		[Test]
		public void UnderOtherType() {
			const string sql = "CREATE OR REPLACE TYPE APP.subType UNDER APP.type1 (id BIGINT) FINAL";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateTypeStatement>(statement);

			var createType = (CreateTypeStatement)statement;

			Assert.IsNotNull(createType.TypeName);
			Assert.IsTrue(createType.ReplaceIfExists);
			Assert.AreEqual("APP.subType", createType.TypeName.FullName);
			Assert.IsNotNull(createType.ParentTypeName);
			Assert.AreEqual("APP.type1", createType.ParentTypeName.FullName);
			Assert.IsFalse(createType.IsAbstract);
			Assert.IsTrue(createType.IsSealed);
			Assert.IsNotNull(createType.Members);
			Assert.IsNotEmpty(createType.Members);
			Assert.AreEqual(1, createType.Members.Length);
		}

		[Test]
		public void NotFinal() {
			const string sql = "CREATE TYPE APP.type1 AS OBJECT (year INT, name VARCHAR) NOT FINAL";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateTypeStatement>(statement);

			var createType = (CreateTypeStatement) statement;

			Assert.IsNotNull(createType.TypeName);
			Assert.IsFalse(createType.ReplaceIfExists);
			Assert.AreEqual("APP.type1", createType.TypeName.FullName);
			Assert.IsNull(createType.ParentTypeName);
			Assert.IsFalse(createType.IsAbstract);
			Assert.IsFalse(createType.IsSealed);
			Assert.IsNotNull(createType.Members);
			Assert.IsNotEmpty(createType.Members);
			Assert.AreEqual(2, createType.Members.Length);
		}
	}
}
