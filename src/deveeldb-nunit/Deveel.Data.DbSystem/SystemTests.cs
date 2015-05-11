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
			Assert.IsTrue(context.IgnoreIdentifiersCase());
			Assert.AreEqual("APP", context.DefaultSchema());
		}

		[Test]
		public void ResolveSingleServiceFromConfig() {
			var config = DbConfig.Default;
			config.SetKey(new ConfigKey("service1", typeof(Type)));
			config.SetValue(new ConfigKey("service1", typeof(Type)), typeof(TestService));

			ISystemContext context = null;
			Assert.DoesNotThrow(() => context = new SystemContext(DbConfig.Default));
			Assert.IsNotNull(context);

			object serviceObj = null;
			Assert.DoesNotThrow(() => serviceObj = context.ServiceProvider.Resolve(typeof(TestService)));
			Assert.IsNotNull(serviceObj);
			Assert.IsInstanceOf<TestService>(serviceObj);

			var service = (TestService) serviceObj;
			Assert.DoesNotThrow(() => service.SayHello());
		}

		#region TestService

		class TestService : IDatabaseService {
			public void SayHello() {
				Console.Out.WriteLine("Hello World.");
			}

			public void Configure(IDbConfig config) {
			}
		}

		#endregion
	}
}
