using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Services;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public class SystemTests {
		[Test]
		public void FromDefaultConfig() {
			ISystemContext context = null;
			Assert.DoesNotThrow(() => context = new SystemContext(Configuration.Configuration.SystemDefault));
			Assert.IsNotNull(context);
			Assert.IsFalse(context.ReadOnly());
			Assert.IsTrue(context.IgnoreIdentifiersCase());
			Assert.AreEqual("APP", context.DefaultSchema());
		}

		[Test]
		public void ResolveSingleServiceFromRegister() {
			ISystemContext context = null;
			Assert.DoesNotThrow(() => context = new SystemContext(Configuration.Configuration.SystemDefault));
			Assert.IsNotNull(context);

			context.ServiceProvider.Register<TestService>();

			object serviceObj = null;
			Assert.DoesNotThrow(() => serviceObj = context.ServiceProvider.Resolve(typeof(TestService)));
			Assert.IsNotNull(serviceObj);
			Assert.IsInstanceOf<TestService>(serviceObj);

			var service = (TestService)serviceObj;
			Assert.DoesNotThrow(() => service.SayHello());
		}

		[Test]
		public void ResolveManyServicesForInterface() {
			ISystemContext context = null;
			Assert.DoesNotThrow(() => context = new SystemContext(Configuration.Configuration.SystemDefault));
			Assert.IsNotNull(context);
			
			context.ServiceProvider.Register<TestService>();
			context.ServiceProvider.Register<TestService2>();
			context.ServiceProvider.Register<TestService3>();

			IEnumerable<ITestService> services = null;
			Assert.DoesNotThrow(() => services = context.ServiceProvider.ResolveAll<ITestService>());
			Assert.IsNotNull(services);

			var serviceList = services.ToList();

			Assert.IsNotEmpty(serviceList);
			Assert.AreEqual(2, serviceList.Count);
			Assert.IsInstanceOf<TestService2>(serviceList[0]);
			Assert.IsInstanceOf<TestService3>(serviceList[1]);
		}

		[Test]
		public void ResolveInstanceOfServiceByInterface() {
			ISystemContext context = null;
			Assert.DoesNotThrow(() => context = new SystemContext(Configuration.Configuration.SystemDefault));
			Assert.IsNotNull(context);

			context.ServiceProvider.Register(new TestService());
			context.ServiceProvider.Register(new TestService2());
			context.ServiceProvider.Register<TestService3>();

			IEnumerable<ITestService> services = null;
			Assert.DoesNotThrow(() => services = context.ServiceProvider.ResolveAll<ITestService>());
			Assert.IsNotNull(services);

			var serviceList = services.ToList();

			Assert.IsNotEmpty(serviceList);
			Assert.AreEqual(2, serviceList.Count);
			Assert.IsInstanceOf<TestService2>(serviceList[0]);
			Assert.IsInstanceOf<TestService3>(serviceList[1]);
		}

		#region TestService

		class TestService {
			public void SayHello() {
				Console.Out.WriteLine("Hello World.");
			}
		}

		#endregion

		#region ITestService

		interface ITestService { 
		}

		#endregion

		#region TestService2

		class TestService2 : ITestService {
			 
		}

		#endregion 

		#region TestService3

		class TestService3 : ITestService { 
		}

		#endregion
	}
}
