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
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	/// <summary>
	/// An isolated session to a given database for a given user,
	/// encapsulating the transaction for operations.
	/// </summary>
	public interface ISession : IContextBased, IDisposable {
		/// <summary>
		/// Gets the name of the current schema of this session.
		/// </summary>
		string CurrentSchema { get; }

		/// <summary>
		/// Gets the user that owns this session.
		/// </summary>
		User User { get; }

		/// <summary>
		/// Gets the instance of <see cref="ITransaction"/> that handles the
		/// transactional operations of this session.
		/// </summary>
		ITransaction Transaction { get; }

		/// <summary>
		/// Gets the <see cref="IContext"/> of the session.
		/// </summary>
        new ISessionContext Context { get; }

		void SetTimeZone(int hours, int minutes);

		/// <summary>
		/// Creates a new large object from the underlying
		/// database of the session.
		/// </summary>
		/// <param name="maxSize">The max size of the object.</param>
		/// <param name="compressed">A flag indicating if the content of the
		/// object will be compressed.</param>
		/// <remarks>
		/// <para>
		/// Large objects are immutable once finalized and the content size
		/// cannot exceed the specified <paramref name="maxSize"/>.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="ILargeObject"/> that is allocated
		/// in the large-object storage of the underlying database of this session.
		/// </returns>
		/// <seealso cref="GetLargeObject"/>
		/// <seealso cref="ILargeObject"/>
		ILargeObject CreateLargeObject(long maxSize, bool compressed);

		/// <summary>
		/// Gets a large object identified by the given unique identifier.
		/// </summary>
		/// <param name="objectId">The unique identifier of the object to obtain.</param>
		/// <returns>
		/// Returns an instance of <see cref="ILargeObject"/> identified by the given
		/// <paramref name="objectId"/> within the underlying database of this session.
		/// </returns>
		/// <seealso cref="ObjectId"/>
		ILargeObject GetLargeObject(ObjectId objectId);

		/// <summary>
		/// Creates a new query object that can be used to execute commands
		/// towards the underlying database of this session.
		/// </summary>
		/// <returns>
		/// Returns an instance of <see cref="IQuery"/> that is used
		/// to execute commands towards the underlying database.
		/// </returns>
		IQuery CreateQuery();


		/// <summary>
		/// Commits the latest changes made by the user in the session.
		/// </summary>
		/// <seealso cref="ITransaction"/>
		void Commit();

		/// <summary>
		/// Rolls-back all the modifications made by the user in this session
		/// </summary>
		/// <seealse cref="ITransaction"/>
		void Rollback();
	}
}