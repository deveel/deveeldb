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
			var builder = new SystemBuilder();
			ISystem system = null;
			Assert.DoesNotThrow(() => system = builder.BuildSystem());
			Assert.IsNotNull(system);
			Assert.IsFalse(system.Context.ReadOnly());
			Assert.IsTrue(system.Context.IgnoreIdentifiersCase());
			Assert.AreEqual("APP", system.Context.DefaultSchema());
		}

		[Test]
		public void ResolveSingleServiceFromRegister() {
			var builder = new SystemBuilder();
			ISystem system = null;
			Assert.DoesNotThrow(() => system = builder.BuildSystem());
			Assert.IsNotNull(system);

			system.Context.RegisterService<TestService>();

			object serviceObj = null;
			Assert.DoesNotThrow(() => serviceObj = system.Context.ResolveService(typeof(TestService)));
			Assert.IsNotNull(serviceObj);
			Assert.IsInstanceOf<TestService>(serviceObj);

			var service = (TestService)serviceObj;
			Assert.DoesNotThrow(() => service.SayHello());
		}

		[Test]
		public void ResolveManyServicesForInterface() {
			var builder = new SystemBuilder();
			ISystem system = null;
			Assert.DoesNotThrow(() => system = builder.BuildSystem());
			Assert.IsNotNull(system);
			
			system.Context.RegisterService<TestService>();
			system.Context.RegisterService<TestService2>();
			system.Context.RegisterService<TestService3>();

			IEnumerable<ITestService> services = null;
			Assert.DoesNotThrow(() => services = system.Context.ResolveAllServices<ITestService>());
			Assert.IsNotNull(services);

			var serviceList = services.ToList();

			Assert.IsNotEmpty(serviceList);
			Assert.AreEqual(2, serviceList.Count);
			Assert.IsInstanceOf<TestService2>(serviceList[0]);
			Assert.IsInstanceOf<TestService3>(serviceList[1]);
		}

		[Test]
		public void ResolveInstanceOfServiceByInterface() {
			var builder = new SystemBuilder();
			ISystem system = null;
			Assert.DoesNotThrow(() => system = builder.BuildSystem());
			Assert.IsNotNull(system);

			system.Context.RegisterInstance(new TestService());
			system.Context.RegisterInstance(new TestService2());
			system.Context.RegisterService<TestService3>();

			IEnumerable<ITestService> services = null;
			Assert.DoesNotThrow(() => services = system.Context.ResolveAllServices<ITestService>());
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
