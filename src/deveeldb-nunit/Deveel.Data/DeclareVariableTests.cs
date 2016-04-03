using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DeclareVariableTests : ContextBasedTest {
		[Test]
		public void SimpleVariableInQueryContext() {
			Query.DeclareVariable("a", PrimitiveTypes.String());

			var obj = Query.Access().GetObject(DbObjectType.Variable, new ObjectName("a"));

			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<Variable>(obj);

			var variable = (Variable) obj;

			Assert.AreEqual("a", variable.Name);
			Assert.IsInstanceOf<StringType>(variable.Type);
		}
	}
}
