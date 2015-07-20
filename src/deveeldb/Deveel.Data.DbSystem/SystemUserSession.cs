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

using Deveel.Data.Caching;
using Deveel.Data.Diagnostics;
using Deveel.Data.Security;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	class SystemUserSession : IUserSession {
		public SystemUserSession(IDatabase database, ITransaction transaction) 
			: this(database, transaction, transaction.CurrentSchema()) {
		}

		public SystemUserSession(IDatabase database, ITransaction transaction, string currentSchema) {
			Database = database;
			CurrentSchema =currentSchema;
			Transaction = transaction;
			SessionInfo = new SessionInfo(User.System);

			TableCache = new MemoryCache(25, 150, 30);
		}

		public void Dispose() {
			Database = null;
			Transaction = null;
		}

		public IDatabase Database { get; private set; }

		public string CurrentSchema { get; private set; }

		public SessionInfo SessionInfo { get; private set; }

		public ITransaction Transaction { get; private set; }

		public ICache TableCache { get; private set; }

		public void Lock(ILockable[] toWrite, ILockable[] toRead, LockingMode mode) {
		}

		public void ReleaseLocks() {
		}

		public ILargeObject CreateLargeObject(long size, bool compressed) {
			throw new NotImplementedException();
		}

		public ILargeObject GetLargeObject(ObjectId objId) {
			throw new NotImplementedException();
		}

		public void Commit() {
			Transaction.Commit();
		}

		public void Rollback() {
			Transaction.Rollback();
		}

		public void AppendEventData(IEvent @event) {
			@event.Database(Database.Name());
			@event.UserName(SessionInfo.User.Name);
		}
	}
}