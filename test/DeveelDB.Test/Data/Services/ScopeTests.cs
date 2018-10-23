// 
//  Copyright 2010-2018 Deveel
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
//

using System;

using Xunit;

namespace Deveel.Data.Services {
	public class ScopeTests : IDisposable {
		private ServiceContainer container;

		public ScopeTests() {
			container = new ServiceContainer();
			container.Register<IService, Service1>("one");
			container.Register<IService, Service2>("two");
		}

		[Fact]
		public void OpenScope() {
			var scope = container.OpenScope("testScope");

			Assert.NotNull(scope);
		}

		[Fact]
		public void OpenScopeAndResolveOneService() {
			var scope = container.OpenScope("testScope");

			var service = scope.Resolve<IService>("one");

			Assert.NotNull(service);
			Assert.IsType<Service1>(service);
		}

		public void Dispose() {
			container.Dispose();
		}

		#region IService

		interface IService {
			
		}

		#endregion

		#region Service1

		class Service1 : IService {
			
		}

		#endregion

		#region Service2

		class Service2 : Service1 {
			
		}

		#endregion
	}
}