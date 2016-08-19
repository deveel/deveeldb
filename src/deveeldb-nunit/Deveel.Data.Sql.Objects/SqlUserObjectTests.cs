using System;
using System.IO;

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Objects {
	[TestFixture]
	public static class SqlUserObjectTests {
		private static UserType Type { get; set; }

		[SetUp]
		public static void TestSetUp() {
			var typeInfo = new UserTypeInfo(ObjectName.Parse("APP.test_type"));
			typeInfo.AddMember("a", PrimitiveTypes.Integer());
			Type = new UserType(typeInfo);
		}

		[Test]
		public static void CreateNewObject() {
			var obj = Type.NewObject(SqlExpression.Constant(34));

			Assert.IsNotNull(obj);
		}

		[Test]
		public static void GetValue() {
			var obj = Type.NewObject(SqlExpression.Constant(34));

			var value = obj.GetValue("a");

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<SqlNumber>(value);
			Assert.AreEqual(34, ((SqlNumber)value).ToInt32());
		}

		[Test]
		public static void GetValueOfKeyNotFond() {
			var obj = Type.NewObject(SqlExpression.Constant(34));

			var value = obj.GetValue("b");

			Assert.IsNotNull(value);
			Assert.IsTrue(value.IsNull);
		}

		[Test]
		public static void Serialize() {
			var obj = Type.NewObject(SqlExpression.Constant(34));

			var serializer = new BinarySerializer();
			var stream = new MemoryStream();
			serializer.Serialize(stream, obj);

			stream.Flush();

			stream.Seek(0, SeekOrigin.Begin);

			var serialized = (SqlUserObject) serializer.Deserialize(stream);

			Assert.IsNotNull(serialized);
			Assert.IsTrue(obj.Equals(serialized));
		}

		[Test]
		public static void Dispose() {
			var obj = Type.NewObject(SqlExpression.Constant(34));

			Assert.DoesNotThrow(() => obj.Dispose());
		}
	}
}
