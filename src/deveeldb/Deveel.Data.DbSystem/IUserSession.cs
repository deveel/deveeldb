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
using System.Data;

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// An isolated session to a given database for a given user,
	/// encapsulating the transaction for operations.
	/// </summary>
	public interface IUserSession : IQueryPlanContext, IDisposable {
		/// <summary>
		/// Gets the instance of the <see cref="User"/> of the session.
		/// </summary>
		User User { get; }

		/// <summary>
		/// Gets the last time the user issued a command within the session.
		/// </summary>
		DateTimeOffset? LastCommand { get; }

		/// <summary>
		/// Gets the transaction that holds the commands from the user.
		/// </summary>
		/// <seealso cref="ITransaction"/>
		ITransaction Transaction { get; }

		IDatabase Database { get; }

		GrantManager GrantManager { get; }

		/// <summary>
		/// Adds a given table to the session cache for fast access.
		/// </summary>
		/// <param name="tableName">The name of the table, used as key for the cache.</param>
		/// <param name="table">The instance of <see cref="ITable"/> to cache.</param>
		/// <see cref="GetCachedTable"/>
		void CacheTable(ObjectName tableName, ITable table);

		/// <summary>
		/// Gets a cached table from the session.
		/// </summary>
		/// <param name="tableName">The name of the table to get.</param>
		/// <returns>
		/// Returns an instance of <see cref="ITable"/> that was previously cached.
		/// </returns>
		/// <seealso cref="CacheTable"/>
		ITable GetCachedTable(ObjectName tableName);


		/// <summary>
		/// Allocates a given amount of memory on the underlying storage system
		/// for the handling of a large-object.
		/// </summary>
		/// <param name="size">The byte size of the object to create.</param>
		/// <param name="compressed">A flag to indicate if the object will handle compressed
		/// data when read or write.</param>
		/// <returns>
		/// Returns an instance of a <see cref="ILargeObject"/> that references the created
		/// object on the underlying storage system.
		/// </returns>
		/// <seealso cref="SqlLongString"/>
		/// <seealso cref="SqlLongBinary"/>
		ILargeObject CreateLargeObject(long size, bool compressed);

		/// <summary>
		/// Gets a large object referenced by the given unique identifier.
		/// </summary>
		/// <param name="objId">The unique identifier of the object to retrieve.</param>
		/// <returns></returns>
		/// <seealso cref="ObjectId"/>
		ILargeObject GetLargeObject(ObjectId objId);

		/// <summary>
		/// Creates an instance of <seealso cref="IDbConnection">an ADO.NET connection</seealso>
		/// used to access the database session through a standard protocol.
		/// </summary>
		/// <returns>
		/// Returns an instance of <seealso cref="IDbConnection"/> that accesses the session.
		/// </returns>
		IDbConnection GetDbConnection();

		/// <summary>
		/// Commits the latest changes made by the user in the session.
		/// </summary>
		/// <seealso cref="ITransaction"/>
		void Commit();

		/// <summary>
		/// 
		/// </summary>
		void Rollback();

		void Close();
	}
}
