using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Types {
	[TestFixture]
	public class UserTypeTest {
		[Test]
		public void CreateNewEmpty() {
			var name = ObjectName.Parse("APP.Type1");
			var type = new UserType(new UserTypeInfo(name));

			Assert.IsNotNull(type);
			Assert.IsNull(type.TypeInfo.ParentType);
			Assert.IsNotNull(type.FullName);
			Assert.AreEqual(name, type.FullName);
			Assert.AreEqual(0, type.MemberCount);
			Assert.IsFalse(type.IsPrimitive);
			Assert.IsFalse(type.IsIndexable);
			Assert.IsFalse(type.IsNull);
		}

		[Test]
		public void CreateSimple() {
			var name = ObjectName.Parse("APP.Type1");
			var typeInfo = new UserTypeInfo(name);
			typeInfo.AddMember("a", PrimitiveTypes.String());
			typeInfo.AddMember("b", PrimitiveTypes.Integer());
			var type = new UserType(typeInfo);

			Assert.IsNotNull(type);
			Assert.IsNull(type.TypeInfo.ParentType);
			Assert.IsNotNull(type.FullName);
			Assert.AreEqual(name, type.FullName);
			Assert.AreEqual(2, type.MemberCount);
			Assert.IsFalse(type.IsPrimitive);
			Assert.IsFalse(type.IsIndexable);
			Assert.IsFalse(type.IsNull);
		}

		[Test]
		public void InstantiateEmptyObject() {
			var name = ObjectName.Parse("APP.Type1");
			var type = new UserType(new UserTypeInfo(name));

			var obj = type.NewObject();

			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<SqlUserObject>(obj);
			Assert.IsFalse(obj.IsNull);
		}

		[Test]
		public void InstantiateSimpleObject() {
			var name = ObjectName.Parse("APP.Type1");
			var typeInfo = new UserTypeInfo(name);
			typeInfo.AddMember("a", PrimitiveTypes.String());
			typeInfo.AddMember("b", PrimitiveTypes.Integer());
			var type = new UserType(typeInfo);

			var obj = type.NewObject(SqlExpression.Constant("test"), SqlExpression.Constant(23));

			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<SqlUserObject>(obj);
			Assert.IsFalse(obj.IsNull);
		}

		[TestCase("MyType")]
		[TestCase("S.MyType")]
		public void ParseUserType(string s) {
			var typeInfo = DataTypeInfo.DefaultParser.Parse(s);

			Assert.IsNotNull(typeInfo);
			Assert.IsNotNull(typeInfo.TypeName);
			Assert.IsFalse(typeInfo.IsPrimitive);
			Assert.AreEqual(s, typeInfo.TypeName);
		}
	}
}
