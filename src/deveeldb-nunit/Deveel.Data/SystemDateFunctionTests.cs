using System;

using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class SystemDateFunctionTests : FunctionTestBase {
		[Test]
		public void CurrentDate() {
			var value = Select("DATE");

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// It is impossible to determine the value of the current date 
			// in this test scenario
		}

		[Test]
		public void CurrentTime() {
			var value = Select("TIME");

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// It is impossible to determine the value of the current date 
			// in this test scenario
		}

		[Test]
		public void CurrentTimeStamp() {
			var value = Select("TIMESTAMP");

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// It is impossible to determine the value of the current date 
			// in this test scenario
		}

		[Test]
		public void SystemDate() {
			var value = Select("SYSTEM_DATE");

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// It is impossible to determine the value of the current date 
			// in this test scenario
		}

		[Test]
		public void SystemTime() {
			var value = Select("SYSTEM_TIME");

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// It is impossible to determine the value of the current date 
			// in this test scenario
		}

		[Test]
		public void SystemTimeStamp() {
			var value = Select("SYSTEM_TIMESTAMP");

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// It is impossible to determine the value of the current date 
			// in this test scenario
		}
	}
}
