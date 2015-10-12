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

using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// The execution context of a database system, that is defining
	/// the configurations and the components used to manage databases.
	/// </summary>
	public interface ISystemContext : IEventSource, IDisposable {
		/// <summary>
		/// Gets the system configuration
		/// </summary>
		IConfiguration Configuration { get; }

		/// <summary>
		/// Gets an instance of <see cref="IEventRegistry"/> that handles
		/// events happening within the context of the database system.
		/// </summary>
		IEventRegistry EventRegistry { get; }

		/// <summary>
		/// Gets an object used to dynamically resolve database services.
		/// </summary>
		/// <seealso cref="ISystemServiceProvider"/>
		ISystemServiceProvider ServiceProvider { get; }
	}
}