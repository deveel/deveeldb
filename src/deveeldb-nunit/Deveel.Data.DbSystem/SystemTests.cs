using System;

using Deveel.Data.Configuration;

using NUnit.Framework;

namespace Deveel.Data.DbSystem {
	[TestFixture]
	public class SystemTests {
		[Test]
		public void FromDefaultConfig() {
			ISystemContext context = null;
			Assert.DoesNotThrow(() => context = new SystemContext(DbConfig.Default));
			Assert.IsNotNull(context);
			Assert.IsFalse(context.ReadOnly());
			Assert.IsFalse(context.IgnoreCase());
			Assert.AreEqual("APP", context.DefaultSchema());
		}
	}
}
