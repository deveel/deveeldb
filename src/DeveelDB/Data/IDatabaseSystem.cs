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
using System.Collections.Generic;
using System.Threading.Tasks;

using Deveel.Data.Configurations;

namespace Deveel.Data {
	/// <summary>
	/// A system to manage database objects and provide
	/// access to them
	/// </summary>
	public interface IDatabaseSystem : IContext, IConfigurationScope {
		/// <summary>
		/// Starts the system making it possible to interact with it
		/// </summary>
		Task StartAsync();

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
		/// <param name="databaseName">The name of the database to create</param>
		/// <param name="configuration">A configuration specific for the
		/// database to be created.</param>
		/// <returns>
		/// Returns an instance of <see cref="IDatabase"/> that is inheriting
		/// the state of this system and is administered by the given user.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the provided <paramref name="databaseName"/> is <c>null</c> or empty.
		/// </exception>
		/// <exception cref="DatabaseSystemException">
		/// If any error occurred that prohibited the creation of the database.
		/// </exception>
		Task<IDatabase> CreateDatabaseAsync(string databaseName, IConfiguration configuration);

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
		Task<bool> DatabaseExistsAsync(string databaseName);

		/// <summary>
		/// Opens an existing database handled by this system.
		/// </summary>
		/// <param name="databaseName">The name of the database to open</param>
		/// <param name="configuration">The configuration that defines the database
		/// to open.</param>
		/// <returns>
		/// Returns an instance of <see cref="IDatabase"/> that represents the database
		/// opened by this system.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="databaseName"/> is <c>null</c> or empty.
		/// </exception>
		/// <exception cref="DatabaseSystemException">
		/// If the database does not exist or if it was not possible to open it.
		/// </exception>
		Task<IDatabase> OpenDatabaseAsync(string databaseName, IConfiguration configuration);

		/// <summary>
		/// Removes a database from the system and deletes it from the underlying
		/// storage system
		/// </summary>
		/// <param name="databaseName">The name of the database to delete</param>
		/// <returns>
		/// Returns <c>true</c> if a database with the given name was found
		/// and deleted, otherwise returns <c>false</c>.
		/// </returns>
		/// <exception cref="DatabaseSystemException">If it was not possible to delete
		/// the database with the given name</exception>
		/// <exception cref="ArgumentNullException">If the specified <paramref name="databaseName"/>
		/// is <c>null</c> or empty</exception>
		Task<bool> DeleteDatabaseAsync(string databaseName);
	}
}