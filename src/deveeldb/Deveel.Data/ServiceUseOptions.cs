// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data.Build;

namespace Deveel.Data {
	public sealed class ServiceUseOptions {
		private Type implType;
		private object instance;
		
		public ServiceUseOptions(Type serviceType) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");

			ServiceType = serviceType;
			Policy = ServiceUsePolicy.Bind;

			if (!ServiceType.IsInterface &&
			    !ServiceType.IsAbstract)
				ImplementationType = ServiceType;
		}


		public Type ServiceType { get; private set; }

		public ServiceUsePolicy Policy { get; set; }

		public Type ImplementationType {
			get { return implType; }
			set {
				CheckImplementationType(value);

				if (value != null)
					instance = null;

				implType = value;
			}
		}

		public object Key { get; set; }

		public string Scope { get; set; }

		public object Instance {
			get { return instance; }
			set {
				CheckInstanceValue(value);

				if (value != null) {
					implType = value.GetType();
				}

				instance = value;
			}
		}

		private void CheckImplementationType(Type type) {
			if (type == null)
				return;

			if (!ServiceType.IsAssignableFrom(type))
				throw new ArgumentException(String.Format("The type '{0}' is not assignable from service type '{1}'.", type, ServiceType));
			if (type.IsAbstract || type.IsInterface)
				throw new ArgumentException(String.Format("The type '{0}' is not instantiable.", type));
		}

		private void CheckInstanceValue(object value) {
			if (value == null)
				return;

			if (!ServiceType.IsInstanceOfType(value))
				throw new ArgumentException(String.Format("The provided instance is not assignable from service type '{0}'",ServiceType));
		}

		internal void Validate() {
			if (instance == null && implType == null) {
				if (ServiceType.IsAbstract ||
				    ServiceType.IsInterface)
					throw new InvalidOperationException(
						String.Format("The type '{0}' is not instantiable and no implementation was specified", ServiceType));
			}
		}
	}
}
