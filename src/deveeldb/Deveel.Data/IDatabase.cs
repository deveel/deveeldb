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

using Deveel.Data.Diagnostics;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	/// <summary>
	/// The representation of a single database in the system.
	/// </summary>
	/// <remarks>
	/// <para>
	/// A database is an assembly of objects of various kind (schemata,
	/// tables, views, types, etc.), organized in a relational model.
	/// </para>
	/// <para>
	/// Implementations of this contract provide functionalities for
	/// <list type="bullet">
	/// <item>Assess the status (version</item>
	/// </list>
	/// </para>
	/// </remarks>
	public interface IDatabase : IEventSource, IDisposable {
		/// <summary>
		/// Gets the context that contains this database.
		/// </summary>
		/// <seealso cref="IDatabaseContext"/>
		IDatabaseContext DatabaseContext { get; }

		/// <summary>
		/// Gets an object that is used to create new transactions to this database
		/// </summary>
		/// <seealso cref="ITransactionFactory"/>
		ITransactionFactory TransactionFactory { get; }

		/// <summary>
		/// Gets the version number of this database.
		/// </summary>
		/// <remarks>
		/// This value is useful for data compatibility between versions
		/// of the system.
		/// </remarks>
		Version Version { get; }

		/// <summary>
		/// Gets a boolean value indicating if the database exists within the
		/// context given.
		/// </summary>
		/// <seealso cref="Create"/>
		bool Exists { get; }

		/// <summary>
		/// Gets a boolean value that indicates if the database was open.
		/// </summary>
		/// <seealso cref="Open"/>
		/// <seealso cref="Close"/>
		bool IsOpen { get; }

		/// <summary>
		/// Gets a special table, unique for every database, that has a single
		/// row and a single cell.
		/// </summary>
		ITable SingleRowTable { get; }

		/// <summary>
		/// Creates the database in the context given, granting the administrative
		/// control to the user identified by the given name and password.
		/// </summary>
		/// <param name="adminName">The name of the administrator.</param>
		/// <param name="adminPassword">The password used to identify the administrator.</param>
		/// <remarks>
		/// <para>
		/// The properties used to create the database are extracted from
		/// the underlying context (<see cref="DatabaseContext"/>).
		/// </para>
		/// <para>
		/// This method does not automatically open the database: to make it accessible
		/// a call to <see cref="Open"/> is required.
		/// </para>
		/// </remarks>
		/// <seealso cref="IDatabaseContext.Configuration"/>
		void Create(string adminName, string adminPassword);

		/// <summary>
		/// Opens the database making it ready to be accessed.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This method ensures the system components and the data are
		/// ready to allow any connection to be established.
		/// </para>
		/// <para>
		/// After this method successfully exists, the state of <see cref="IsOpen"/>
		/// is changed to <c>true</c>.
		/// </para>
		/// </remarks>
		void Open();

		/// <summary>
		/// Closes the database making it not accessible to connections.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Typical implementations of this interface will automatically
		/// invoke the closure of the database on disposal (<see cref="IDisposable.Dispose"/>.
		/// </para>
		/// </remarks>
		void Close();
	}
}
