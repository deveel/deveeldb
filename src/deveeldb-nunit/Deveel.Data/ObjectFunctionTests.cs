using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public class ObjectFunctionTests : FunctionTestBase {
		protected override bool OnSetUp(string testName, IQuery query) {
			var typeName = ObjectName.Parse("APP.test_type1");
			var typeInfo = new UserTypeInfo(typeName);
			typeInfo.AddMember("a", PrimitiveTypes.String());
			typeInfo.AddMember("b", PrimitiveTypes.Integer());

			query.Access().CreateObject(typeInfo);

			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var typeName = ObjectName.Parse("APP.test_type1");
			query.Access().DropObject(DbObjectType.Type, typeName);
			return true;
		}

		[Test]
		public void CreateObjectImplicit() {
			var result = Select("test_type1", SqlExpression.Constant("test"), SqlExpression.Constant(22));

			Assert.IsFalse(Field.IsNullField(result));
			Assert.IsInstanceOf<UserType>(result.Type);
			Assert.IsInstanceOf<SqlUserObject>(result.Value);
		}
	}
}
