using System;
using System.Linq;

using NUnit.Framework;

namespace Deveel.Data.Services {
	[TestFixture]
	public class ServiceContainerTests {
		[Test]
		public void RegisterAndResolveFromChild() {
			var parent = new ServiceContainer();
			parent.Register<TestService1>();

			var child = parent.OpenScope("child");

			var childService = child.Resolve<ITestService>();
			var parentService = parent.Resolve<ITestService>();

			Assert.IsNotNull(childService);
			Assert.IsInstanceOf<TestService1>(childService);

			Assert.IsNotNull(parentService);
			Assert.IsInstanceOf<TestService1>(parentService);

			Assert.AreEqual(parentService, childService);
		}

		[Test]
		public void RegisterInstanceAndResolveFromChild() {
			var instance = new TestService1();

			var parent = new ServiceContainer();
			parent.RegisterInstance(instance);

			var child = parent.OpenScope("child");

			var childService = child.Resolve<ITestService>();

			Assert.IsNotNull(childService);
			Assert.IsInstanceOf<TestService1>(childService);
			Assert.AreEqual(instance, childService);
		}

		[Test]
		public void RegisterInstanceAndResolveAllFromChild() {
			var instance = new TestService1();

			var parent = new ServiceContainer();
			parent.RegisterInstance(instance);

			var child = parent.OpenScope("child");

			var services = child.ResolveAll<ITestService>();

			Assert.IsNotEmpty(services);
			Assert.AreEqual(1, services.Count());
		}

		[Test]
		public void ResolveFromChildWithParentService() { 
			var parent = new ServiceContainer();
			parent.Register<TestService2>();

			var child = parent.OpenScope("child");
			child.Register<TestService1>();

			var service2 = child.Resolve<TestService2>();

			Assert.IsNotNull(service2);
			Assert.IsNotNull(service2.Service1);
		}

		#region Context

		class Context : Services.Context {
			protected override string ContextName {
				get { return "TestContext"; }
			}
		}

		#endregion

		#region ITestService

		interface ITestService {
		}

		#endregion

		#region TestService1

		class TestService1 : ITestService {
			 
		}

		#endregion

		#region TestService2

		class TestService2 {
			public TestService2(TestService1 service1) {
				Service1 = service1;
			}

			public TestService1 Service1 { get; private set; }
		}

		#endregion
	}
}
