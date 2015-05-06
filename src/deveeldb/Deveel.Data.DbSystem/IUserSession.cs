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

using Deveel.Data.Sql.Objects;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// An isolated session to a given database for a given user,
	/// encapsulating the transaction for operations.
	/// </summary>
	public interface IUserSession : IDisposable {
		IDatabase Database { get; }

		string CurrentSchema { get; }

		SessionInfo SessionInfo { get; }

		ITransaction Transaction { get; }


		void Lock(ILockable[] toWrite, ILockable[] toRead, LockingMode mode);

		void ReleaseLocks();


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
		/// Commits the latest changes made by the user in the session.
		/// </summary>
		/// <seealso cref="ITransaction"/>
		void Commit();

		/// <summary>
		/// 
		/// </summary>
		void Rollback();
	}
}
