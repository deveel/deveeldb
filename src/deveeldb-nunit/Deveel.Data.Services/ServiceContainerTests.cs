using System;

using NUnit.Framework;

namespace Deveel.Data.Services {
	[TestFixture]
	public class ServiceContainerTests {
		[Test]
		public void RegisterAndResolveFromChild() {
			var context1 = new Context();
			var parent = new ServiceContainer(context1);
			parent.Register<TestService1>();

			var context2 = new Context();
			var child = new ServiceContainer(context2, parent);

			var childService = child.Resolve<ITestService>();
			var parentService = parent.Resolve<ITestService>();

			Assert.IsNotNull(childService);
			Assert.IsInstanceOf<TestService1>(childService);

			Assert.IsNotNull(parentService);
			Assert.IsInstanceOf<TestService1>(parentService);

			Assert.AreNotEqual(parentService, childService);
		}

		#region Context

		class Context { 
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
	}
}
