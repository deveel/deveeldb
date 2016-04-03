using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CreateTypeTests : ContextBasedTest {
		[Test]
		public void SimpleType() {
			var typeName = ObjectName.Parse("APP.type1");
			Query.CreateType(typeName, new[] {new UserTypeMember("a", PrimitiveTypes.String())});

			var exists = Query.Access().TypeExists(typeName);

			Assert.IsTrue(exists);
		}
	}
}
