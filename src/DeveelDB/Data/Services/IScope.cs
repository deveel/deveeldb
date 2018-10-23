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
using System.Collections;

namespace Deveel.Data.Services {
	public interface IScope : IServiceProvider, IDisposable {
		string Name { get; }

		/// <summary>
		/// Resolves an instance of the service of the given type
		/// contained in this scope
		/// </summary>
		/// <param name="serviceType">The type of the service to resolve</param>
		/// <param name="serviceKey">An optional key for the service to be resolved,
		/// to discriminate between two services of the same type.</param>
		/// <returns>
		/// Returns an instance of the service for the given <paramref name="serviceType"/>
		/// that was registered at build.
		/// </returns>
		/// <seealso cref="IServiceContainer.Register"/>
		/// <exception cref="ServiceResolutionException">Thrown if an error occurred
		/// while resolving the service within this scope</exception>
		object Resolve(Type serviceType, object serviceKey);

		/// <summary>
		/// Resolves all the instances of services of the given type 
		/// contained in this scope
		/// </summary>
		/// <param name="serviceType">The type of the service instances to be resolved</param>
		/// <returns>
		/// Returns an enumeration of instances of all the services of the given
		/// <paramref name="serviceType"/> contained in this scope
		/// </returns>
		/// <seealso cref="IServiceContainer.Register"/>
		IEnumerable ResolveAll(Type serviceType);
	}
}