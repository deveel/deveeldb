// 
//  Copyright 2010-2015 Deveel
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

namespace Deveel.Data {
	/// <summary>
	/// A special <see cref="IServiceProvider"/> that provides IoC (Inversion
	/// of Control) features to resolve services for database components.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Some features of a database system, including third party components,
	/// might require the resolution of extension services to be operative.
	/// </para>
	/// <para>
	/// This object provides a fast way to instantiate and resolve components
	/// by a given <see cref="Type"/> or by its unique name.
	/// </para>
	/// <para>
	/// A typical implementation of this interface allows the definition of services 
	/// at configuration, allowing the resolution at runtime.
	/// </para>
	/// </remarks>
	/// <seealso cref="IServiceProvider"/>
	public interface ISystemServiceProvider : IServiceProvider, IDisposable {
		void AttachContext(IServiceResolveContext context);

		/// <summary>
		/// Resolves a service by its type and instance name.
		/// </summary>
		/// <param name="serviceType">The type of the service to resolve.</param>
		/// <param name="name">The name of the instance of the service to resolve.</param>
		/// <remarks>
		/// This methods first attempts to resolve an instance for the given
		/// <see cref="Type"/>, and then if multiple instances are configured for that
		/// type it tries to resolve one named instance having the same as the one
		/// specified as argument of this method.
		/// </remarks>
		/// <returns>
		/// Returns an object that is the instance of the given <paramref name="serviceType"/>
		/// and for the given name, or <c>null</c> if 
		/// </returns>
		object Resolve(Type serviceType, string name);

		/// <summary>
		/// Resolves all instances of services of the given type.
		/// </summary>
		/// <param name="serviceType">The <see cref="Type"/> of the services to
		/// resolve within the provider context.</param>
		/// <returns>
		/// Returns an enumerable object of all the instances found in the context
		/// that implement the given type.
		/// </returns>
		IEnumerable ResolveAll(Type serviceType);

		/// <summary>
		/// Registers a service with the given type, optionally identified by
		/// a given name.
		/// </summary>
		/// <param name="name">The optional name identifier of the service.</param>
		/// <param name="serviceType">The type of the service to register.</param>
		/// <param name="service">An optional instance of the service</param>
		/// <exception cref="ArgumentNullException">
		/// If the provided <paramref name="serviceType"/> is <c>null</c>.
		/// </exception>
		/// <exception cref="ArgumentException">
		/// If the provided <paramref name="service"/> is <c>not null</c>
		/// and it is not an instance of <paramref name="serviceType"/>.
		/// </exception>
		void Register(string name, Type serviceType, object service);
	}
}
