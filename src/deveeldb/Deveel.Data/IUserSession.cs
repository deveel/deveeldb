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
using System.Collections.Generic;

using Deveel.Data.Diagnostics;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	/// <summary>
	/// An isolated session to a given database for a given user,
	/// encapsulating the transaction for operations.
	/// </summary>
	public interface IUserSession : IEventSource, IDisposable {
		/// <summary>
		/// Gets the name of the current schema of this session.
		/// </summary>
		string CurrentSchema { get; }

		DateTimeOffset StartedOn { get; }

		DateTimeOffset? LastCommandTime { get; }

		User User { get; }

		/// <summary>
		/// Gets the instance of <see cref="ITransaction"/> that handles the
		/// transactional operations of this session.
		/// </summary>
		ITransaction Transaction { get; }

        ISessionContext SessionContext { get; }


		///// <summary>
		///// Allocates a given amount of memory on the underlying storage system
		///// for the handling of a large-object.
		///// </summary>
		///// <param name="size">The byte size of the object to create.</param>
		///// <param name="compressed">A flag to indicate if the object will handle compressed
		///// data when read or write.</param>
		///// <returns>
		///// Returns an instance of a <see cref="ILargeObject"/> that references the created
		///// object on the underlying storage system.
		///// </returns>
		///// <seealso cref="SqlLongString"/>
		///// <seealso cref="SqlLongBinary"/>
		//ILargeObject CreateLargeObject(long size, bool compressed);

		///// <summary>
		///// Gets a large object referenced by the given unique identifier.
		///// </summary>
		///// <param name="objId">The unique identifier of the object to retrieve.</param>
		///// <returns></returns>
		///// <seealso cref="ObjectId"/>
		//ILargeObject GetLargeObject(ObjectId objId);

		void Access(IEnumerable<IDbObject> objects, AccessType accessType);

		void Exit(IEnumerable<IDbObject> objects, AccessType accessType);

		void Lock(IEnumerable<IDbObject> objects, AccessType accessType, LockingMode mode);

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