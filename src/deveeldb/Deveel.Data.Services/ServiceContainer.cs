using System;
using System.Collections;
using System.Collections.Generic;

using DryIoc;

namespace Deveel.Data.Services {
	public class ServiceContainer : IServiceContainer, IServiceProvider {
		private IContainer container;

		public ServiceContainer(object context) 
			: this(context, null) {
		}

		public ServiceContainer(object context, ServiceContainer parent) {
			if (context == null)
				throw new ArgumentNullException("context");

			if (parent != null) {
				var scopeName = String.Format("{0}.Scope", context.GetType().Name);
				container = parent.container.OpenScope(scopeName).WithoutSingletonsAndCache();
			} else {
				container = new Container(Rules.Default
					.WithDefaultReuseInsteadOfTransient(Reuse.Singleton)
					.WithoutThrowOnRegisteringDisposableTransient());
			}

			Register(context.GetType(), null, context);

			Context = context;
			Parent = parent;
		}

		~ServiceContainer() {
			Dispose(false);
		}

		public object Context { get; private set; }

		public ServiceContainer Parent { get; private set; }

		object IServiceProvider.GetService(Type serviceType) {
			return Resolve(serviceType, null);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				lock (this) {
					if (container != null)
						container.Dispose();
				}
			}

			container = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public object Resolve(Type serviceType, string name) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");

			if (container == null)
				throw new InvalidOperationException("The container was not initialized.");

			lock (this) {
				return container.Resolve(serviceType, name, IfUnresolved.ReturnDefault);
			}
		}

		public IEnumerable ResolveAll(Type serviceType) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");

			if (container == null)
				throw new InvalidOperationException("The container was not initialized.");

			lock (this) {
				return container.ResolveMany<object>(serviceType);
			}
		}

		public void Register(Type serviceType, string serviceName, object service) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");

			if (container == null)
				throw new InvalidOperationException("The container was not initialized.");

			lock (this) {
				if (!serviceType.IsAbstract && !serviceType.IsInterface) {
					var ifaces = serviceType.GetInterfaces();
					foreach (var iface in ifaces) {
						if (service != null) {
							container.RegisterInstance(iface, service, serviceKey:serviceName);
						} else {
							container.Register(iface, serviceType, serviceKey: serviceName);
						}
					}
				}

				if (service == null) {
					container.Register(serviceType, serviceKey: serviceName);
				} else {
					container.RegisterInstance(serviceType, service, serviceKey: serviceName);
				}
			}
		}

		public void Unregister(Type serviceType, string serviceName) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");

			if (container == null)
				throw new InvalidOperationException("The container was not initialized.");

			lock (this) {
				container.Unregister(serviceType, serviceName);
			}
		}

		//#region ServiceScopeContext

		//class ServiceScopeContext : IScopeContext {
		//	private IScope currentScope;


		//	~ServiceScopeContext() {
		//		Dispose(false);
		//	}

		//	public void Dispose() {
		//		Dispose(true);
		//		GC.SuppressFinalize(this);
		//	}

		//	private void Dispose(bool disposing) {
		//		if (disposing) {
		//			if (currentScope != null)
		//				currentScope.Dispose();
		//		}

		//		currentScope = null;
		//	}

		//	public IScope GetCurrentOrDefault() {
		//		return currentScope;
		//	}

		//	public IScope SetCurrent(SetCurrentScopeHandler setCurrentScope) {
		//		var newScope = setCurrentScope(currentScope);
		//		if (newScope != null)
		//			currentScope = newScope;

		//		return currentScope;
		//	}

		//	public string RootScopeName {
		//		get { return "ServiceContext"; }
		//	}
		//}

		//#endregion

		//#region ContextScope

		//class ContextScope : IScope {
		//	private Dictionary<int, object> values;

		//	public ContextScope(IScope parent) {
		//		Parent = parent;
		//	}

		//	~ContextScope() {
		//		Dispose(false);
		//	} 

		//	public void Dispose() {
		//		Dispose(true);
		//		GC.SuppressFinalize(this);
		//	}

		//	private void Dispose(bool disposing) {
		//		if (disposing) {
		//			if (values != null) {
		//				foreach (var value in values.Values) {
		//					if (value is IDisposable)
		//						((IDisposable)value).Dispose();
		//				}

		//				values.Clear();
		//			}
		//		}

		//		values = null;
		//	}

		//	public object GetOrAdd(int id, CreateScopedValue createValue) {
		//		if (values == null)
		//			return null;

		//		object value;
		//		if (!values.TryGetValue(id, out value)) {
		//			value = createValue();
		//			values[id] = value;
		//		}

		//		return value;
		//	}

		//	public void SetOrAdd(int id, object item) {
		//		if (values == null)
		//			return;

		//		values[id] = item;
		//	}

		//	public int GetScopedItemIdOrSelf(int externalId) {
		//		return externalId;
		//	}

		//	public IScope Parent { get; private set; }

		//	public object Name {
		//		get { return "ContextScope"; }
		//	}
		//}

		//#endregion
	}
}
