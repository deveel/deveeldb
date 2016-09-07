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

using Deveel.Data.Diagnostics;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Store;
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
	public interface IDatabase : IDisposable {
		/// <summary>
		/// Gets the name of the database.
		/// </summary>
		string Name { get; }

		/// <summary>
		/// Gets the context that contains this database.
		/// </summary>
		/// <seealso cref="IDatabaseContext"/>
		IDatabaseContext Context { get; }

		ISystem System { get; }

		/// <summary>
		/// Gets a registry of statistical counters 
		/// </summary>
		ICounterRegistry Counters { get; }

		/// <summary>
		/// Gets an object that is used to create new transactions to this database
		/// </summary>
		/// <seealso cref="ITransactionFactory"/>
		ITransactionFactory TransactionFactory { get; }

		/// <summary>
		/// Gets a list of all the open sessions
		/// to the database.
		/// </summary>
		/// <value>
		/// The open sessions to the database.
		/// </value>
		ActiveSessionList Sessions { get; }

		/// <summary>
		/// Gets the objects that is used to lock database 
		/// objects between transactions.
		/// </summary>
		/// <value>
		/// The database object locker.
		/// </value>
		Locker Locker { get; }

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

		ILargeObject CreateLargeObject(long objectSize, bool compressed);

		ILargeObject GetLargeObject(ObjectId objId);
	}
}
