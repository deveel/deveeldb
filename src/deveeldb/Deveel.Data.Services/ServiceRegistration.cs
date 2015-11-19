using System;

namespace Deveel.Data.Services {
	public sealed class ServiceRegistration {
		private object instance;

		public ServiceRegistration(Type serviceType, Type implementationType) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");
			if (implementationType == null)
				throw new ArgumentNullException("implementationType");

			if (!serviceType.IsAssignableFrom(implementationType))
				throw new ArgumentException(
					String.Format("The implementation type '{0} is not assignable from the service type '{1}'.",
						implementationType, serviceType));

			ServiceType = serviceType;
			ImplementationType = implementationType;
		}

		public Type ServiceType { get; private set; }

		public Type ImplementationType { get; private set; }

		public object ServiceKey { get; set; }

		public string Scope { get; set; }

		public object Instance {
			get { return instance; }
			set {
				if (value != null &&
					!ServiceType.IsInstanceOfType(value))
					throw new ArgumentException(String.Format("The instance is not assignable from '{0}'.", ServiceType));

				instance = value;
			}
		}
	}
}
