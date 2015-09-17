using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Routines {
	[TestFixture]
	public class SystemFunctionTests : ContextBasedTest {
		private DataObject InvokeFunction(string name) {
			return QueryContext.InvokeSystemFunction(name);
		}

		private DataObject InvokeFunction(string name, DataObject arg) {
			return QueryContext.InvokeSystemFunction(name, SqlExpression.Constant(arg));
		}

		[Test]
		public void ResolveSystemFunctionWithNoSchema() {
			IFunction function = null;
			Assert.DoesNotThrow(() => function = QueryContext.ResolveFunction(new ObjectName("user")));
			Assert.IsNotNull(function);
			Assert.AreEqual(SystemSchema.Name, function.FullName.ParentName);
			Assert.AreEqual("user", function.FullName.Name);
		}

		[Test]
		public void ResolveSystemFunctionFullyQualified() {
			IFunction function = null;
			Assert.DoesNotThrow(() => function = QueryContext.ResolveFunction(ObjectName.Parse("SYSTEM.user")));
			Assert.IsNotNull(function);
			Assert.AreEqual(SystemSchema.Name, function.FullName.ParentName);
			Assert.AreEqual("user", function.FullName.Name);
		}

		[Test]
		public void InvokeUserFunction() {
			DataObject result = null;
			Assert.DoesNotThrow(() => result = InvokeFunction("user"));
			Assert.IsNotNull(result);
			Assert.AreEqual(AdminUserName, result.Value.ToString());
		}

		[Test]
		public void InvokeIntegerToString() {
			var value = DataObject.Integer(455366);
			DataObject result = null;
			Assert.DoesNotThrow(() => result = InvokeFunction("TOSTRING", value));
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<StringType>(result.Type);

			var stringResult = result.Value.ToString();
			Assert.AreEqual("455366", stringResult);
		}

		[Test]
		public void InvokeDateToString() {
			var value = DataObject.Date(new SqlDateTime(2015, 02, 10));
			DataObject result = null;
			Assert.DoesNotThrow(() => result = InvokeFunction("TOSTRING", value));
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<StringType>(result.Type);

			var stringResult = result.Value.ToString();
			Assert.AreEqual("2015-02-10", stringResult);
		}

		[Test]
		public void InvokeTimeStampToString_NoFormat() {
			var value = DataObject.TimeStamp(new SqlDateTime(2015, 02, 10, 17, 15, 01,00));
			DataObject result = null;
			Assert.DoesNotThrow(() => result = InvokeFunction("TOSTRING", value));
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<StringType>(result.Type);

			var stringResult = result.Value.ToString();
			Assert.AreEqual("2015-02-10T17:15:01.000 +00:00", stringResult);
		}
	}
}
