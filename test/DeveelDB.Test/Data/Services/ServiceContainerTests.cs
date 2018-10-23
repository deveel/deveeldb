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
using System.Linq;

using Xunit;

namespace Deveel.Data.Services {
	public class ServiceContainerTests {
		[Theory]
		[InlineData(typeof(IService), typeof(ServiceOne), null)]
		[InlineData(typeof(IService), typeof(ServiceOne), "s1")]
		public void RegisterAndCheckService(Type serviceType, Type implementationType, object key) {
			var provider = new ServiceContainer();
			provider.Register(serviceType, implementationType, key);

			Assert.True(provider.IsRegistered(serviceType, key));
		}

		[Theory]
		[InlineData(typeof(IService), typeof(ServiceOne), null)]
		[InlineData(typeof(IService), typeof(ServiceOne), "s1")]
		public void RegisterAndResolveService(Type serviceType, Type implementationType, object key) {
			var provider = new ServiceContainer();
			provider.Register(serviceType, implementationType, key);

			var service = provider.Resolve(serviceType, key);

			Assert.NotNull(service);
			Assert.IsType(implementationType, service);
		}

		[Theory]
		[InlineData(typeof(ServiceOne), null)]
		[InlineData(typeof(ServiceOne), "s1")]
		public void RegisterAndResolveServiceClass(Type serviceType, object key) {
			var provider = new ServiceContainer();
			provider.Register(serviceType, key);

			var service = provider.Resolve(serviceType, key);

			Assert.NotNull(service);
			Assert.IsType(serviceType, service);
		}

		[Fact]
		public void RegisterNullService() {
			var provider = new ServiceContainer();
			Assert.Throws<ArgumentNullException>(() => provider.Register(null));
		}

		[Fact]
		public void RegisterNonInstantiableService() {
			var provider = new ServiceContainer();
			Assert.Throws<ServiceException>(() => provider.Register<IService>());
		}

		[Fact]
		public void OpenScopeAndResolveParent() {
			var provider = new ServiceContainer();
			provider.Register<IService, ServiceOne>();

			var scope = provider.OpenScope("c");

			Assert.NotNull(scope);

			var service = provider.Resolve<IService>();

			Assert.NotNull(service);
			Assert.IsType<ServiceOne>(service);
		}

		[Fact]
		public void RegisterAndUnregisterFromSameScope() {
			var provider = new ServiceContainer();
			provider.Register<IService, ServiceOne>();

			Assert.True(provider.IsRegistered<IService>());

			Assert.True(provider.Unregister<IService>());

			Assert.False(provider.IsRegistered<IService>());
			var service = provider.Resolve<IService>();

			Assert.Null(service);
		}

		[Fact]
		public void RegisterAndUnregisterFromChildScope() {
			var provider = new ServiceContainer();
			provider.Register<IService, ServiceOne>();

			Assert.True(provider.IsRegistered<IService>());
			Assert.True(provider.Unregister<IService>());

			var service = provider.Resolve<IService>();

			Assert.Null(service);
		}

		[Fact]
		public void ResolveAll() {
			var provider = new ServiceContainer();
			provider.Register<IService, ServiceOne>();
			provider.Register<IService, ServiceTwo>();

			var services = provider.ResolveAll<IService>();

			Assert.NotNull(services);
			Assert.NotEmpty(services);
			Assert.Equal(2, services.Count());
		}

		[Fact]
		public void ResolveNotRegistered() {
			var provider = new ServiceContainer();
			var service = provider.Resolve<IService>();

			Assert.Null(service);
		}

		[Fact]
		public void ResolveNullService() {
			var provider = new ServiceContainer();
			Assert.Throws<ArgumentNullException>(() => provider.Resolve(null));
		}

		[Fact]
		public void RegisterManyAndResolveOne() {
			var provider = new ServiceContainer();
			provider.Register<IService, ServiceOne>();
			provider.Register<IService, ServiceTwo>("two");

			var service = provider.Resolve<IService>("two");

			Assert.NotNull(service);
			Assert.IsType<ServiceTwo>(service);

			Assert.Equal("Hello!", service.Do());
		}

		[Fact]
		public void RegisterInstance() {
			var provider = new ServiceContainer();
			provider.RegisterInstance<IService>(new ServiceOne());

			var service = provider.Resolve<IService>();

			Assert.NotNull(service);
			Assert.IsType<ServiceOne>(service);
		}

		[Fact]
		public void RegisterNullInstance() {
			var provider = new ServiceContainer();
			Assert.Throws<ArgumentNullException>(() => provider.RegisterInstance<IService>(null));
		}

		[Fact]
		public void RegisterInstanceExplicit() {
			var provider = new ServiceContainer();
			provider.RegisterInstance(typeof(IService), new ServiceOne());

			var service = provider.Resolve<IService>();

			Assert.NotNull(service);
			Assert.IsType<ServiceOne>(service);
		}

		[Fact]
		public void ReplaceService() {
			var provider = new ServiceContainer();
			provider.Register<IService, ServiceOne>();

			Assert.True(provider.Replace<IService, ServiceTwo>());

			var service = provider.Resolve<IService>();
			Assert.NotNull(service);
			Assert.IsType<ServiceTwo>(service);
		}


		[Fact]
		public void RegisterNonInstantiable() {
			var provider = new ServiceContainer();
			Assert.Throws<ServiceException>(() => provider.Register<IService>());
		}

		[Fact]
		public void DisposeProvider() {
			var provider = new ServiceContainer();
			provider.Register<IService, DisposableService>();

			var service = provider.Resolve<IService>();

			Assert.IsType<DisposableService>(service);

			var value = service.Do();
			Assert.Equal("I'm alive!", value);

			provider.Dispose();

			Assert.True(((DisposableService)service).Disposed);
			Assert.Throws<ObjectDisposedException>(() => service.Do());
		}

		[Fact]
		public void ResolveAfterDispose() {
			var provider = new ServiceContainer();
			provider.Register<IService, DisposableService>();

			provider.Dispose();

			Assert.Throws<InvalidOperationException>(() => provider.Resolve<IService>());
		}

		[Fact]
		public void RegisterAfterDispose() {
			var provider = new ServiceContainer();
			provider.Register<IService, DisposableService>();

			provider.Dispose();

			Assert.Throws<InvalidOperationException>(() => provider.Register<IService, ServiceOne>());
		}

		[Fact]
		public void IsRegisteredAfterDispose() {
			var provider = new ServiceContainer();
			provider.Register<IService, DisposableService>();

			provider.Dispose();

			Assert.Throws<InvalidOperationException>(() => provider.IsRegistered<IService>());
		}

		[Fact]
		public void IsRegisteredWithNullService() {
			var provider = new ServiceContainer();
			provider.Register<IService, DisposableService>();

			Assert.Throws<ArgumentNullException>(() => provider.IsRegistered(null));
		}

		#region IService

	private interface IService {
			object Do();
		}

		#endregion

		#region ServiceOne

		class ServiceOne : IService {
			public object Do() {
				return 566;
			}
		}

		#endregion

		#region ServiceTwo

		class ServiceTwo : IService {
			public object Do() {
				return "Hello!";
			}
		}

		#endregion

		#region DisposableService

		class DisposableService : IService, IDisposable {
			public bool Disposed { get; private set; }

			private void AssertNotDisposed() {
				if (Disposed)
					throw new ObjectDisposedException(GetType().Name);
			}

			public object Do() {
				AssertNotDisposed();
				return "I'm alive!";
			}

			public void Dispose() {
				Disposed = true;
			}
		}

		#endregion
	}
}