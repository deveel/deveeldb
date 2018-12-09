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
using System.Threading.Tasks;

using Deveel.Data.Configurations;

namespace Deveel.Data {
	/// <summary>
	/// Defines the properties and functions of an engine that
	/// creates and manages <see cref="IDatabase">database</see> instances
	/// within the scope of a <see cref="IDatabaseSystem"/>
	/// </summary>
	public interface IDatabaseEngine {
		/// <summary>
		/// Creates a new database using the configurations provided
		/// </summary>
		/// <param name="configuration">The configurations used by the engine
		/// to create the database.</param>
		/// <returns>
		/// Returns an instance of the <see cref="IDatabase"/> created by the system
		/// </returns>
		/// <exception cref="SystemException">Thrown if an unknown error occurred while
		/// creating the database instance</exception>
		Task<IDatabase> CreateDatabaseAsync(IConfiguration configuration);

		Task<bool> DeleteDatabaseAsync(IConfiguration configuration);

		Task<IDatabase> OpenDatabaseAsync(IConfiguration configuration);
	}
}