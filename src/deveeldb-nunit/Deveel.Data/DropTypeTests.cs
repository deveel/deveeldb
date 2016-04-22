using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropTypeTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			var typeName = ObjectName.Parse("APP.type1");
			var typeInfo = new UserTypeInfo(typeName);
			typeInfo.AddMember("a", PrimitiveTypes.Integer());
			typeInfo.AddMember("b", PrimitiveTypes.DateTime());
			query.Access().CreateObject(typeInfo);

			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var typeName = ObjectName.Parse("APP.type1");
			query.Access().DropObject(DbObjectType.Type, typeName);
			return true;
		}

		[Test]
		public void SimpleTypeThatExists() {
			var typeName = ObjectName.Parse("APP.type1");
			Query.DropType(typeName);

			var exists = Query.Access().ObjectExists(DbObjectType.Type, typeName);

			Assert.IsFalse(exists);
		}

		[Test]
		public void IfExists() {
			var typeName = ObjectName.Parse("APP.type2");
			Query.DropType(typeName, true);

			var exists = Query.Access().ObjectExists(DbObjectType.Type, typeName);
			Assert.IsFalse(exists);
		}
	}
}
