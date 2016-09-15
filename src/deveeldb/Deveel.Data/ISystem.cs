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
using System.Collections.Generic;

using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// Provides the interface for the definition of a database system.
	/// </summary>
	/// <remarks>
	/// <para>
	/// A database system handles the creation and the reference
	/// to existing <see cref="IDatabase">databases</see>.
	/// </para>
	/// </remarks>
	public interface ISystem : IContextBased, IDatabaseHandler, IDisposable {
		/// <summary>
		/// Gets a list of the modules handled by the system.
		/// </summary>
		IEnumerable<ModuleInfo> Modules { get; }
		 
		/// <summary>
		/// Gets a context that provides the state of the system.
		/// </summary>
		new ISystemContext Context { get; }

		/// <summary>
		/// Gets a list of the names of all the databases handled
		/// by this system.
		/// </summary>
		/// <returns>
		/// Returns an enumeration of <see cref="string"/> that
		/// provides the names of the databases handled by this system.
		/// </returns>
		IEnumerable<string> GetDatabases();
			
		/// <summary>
		/// Creates a new database with the given configuration
		/// within this database system.
		/// </summary>
		/// <param name="configuration">A configuration specific for the
		/// database to be created.</param>
		/// <param name="adminUser">The name of the administrator of the database.</param>
		/// <param name="identification">The name of the mechanism used to identify the
		/// user with the given token.</param>
		/// <param name="token">The token to identify the administrator user.</param>
		/// <returns>
		/// Returns an instance of <see cref="IDatabase"/> that is inheriting
		/// the state of this system and is administered by the given user.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the provided <paramref name="configuration"/> object if <c>null</c>.
		/// </exception>
		/// <exception cref="ArgumentException">
		/// If the given <paramref name="configuration"/> does not specify any
		/// database name.
		/// </exception>
		/// <exception cref="DatabaseSystemException">
		/// If any error occurred that prohibited the creation of the database.
		/// </exception>
		IDatabase CreateDatabase(IConfiguration configuration, string adminUser, string identification, string token);

		/// <summary>
		/// Checks if any database with the given name exists 
		/// within this system.
		/// </summary>
		/// <param name="databaseName">The name of the database to check.</param>
		/// <remarks>
		/// <para>
		/// This method verifies that any database with the given <paramref name="databaseName">name</paramref>
		/// is handled by this system and that it exists in the underlying storage.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns <c>true</c> if a database with the given name is handled by
		/// this system and exists in the underlying storage.
		/// </returns>
		bool DatabaseExists(string databaseName);

		/// <summary>
		/// Opens an existing database handled by this system.
		/// </summary>
		/// <param name="configuration">The configuration that defines the database
		/// to open.</param>
		/// <returns>
		/// Returns an instance of <see cref="IDatabase"/> that represents the database
		/// opened by this system.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="configuration"/> is <c>null</c>.
		/// </exception>
		/// <exception cref="ArgumentException">
		/// If the given <paramref name="configuration"/> does not provide any
		/// database name.
		/// </exception>
		/// <exception cref="DatabaseSystemException">
		/// If the database does not exist or if it was not possible to open it.
		/// </exception>
		IDatabase OpenDatabase(IConfiguration configuration);

		bool DeleteDatabase(string databaseName);
	}
}
