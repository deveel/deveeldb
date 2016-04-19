// 
//  Copyright 2010-2014 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

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
