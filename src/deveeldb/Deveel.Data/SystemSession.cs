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

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	class SystemSession : ISession, IProvidesDirectAccess {
		private readonly DateTimeOffset startedOn;

		public SystemSession(ITransaction transaction) 
			: this(transaction, transaction.CurrentSchema()) {
		}

		public SystemSession(ITransaction transaction, string currentSchema) {
			if (String.IsNullOrEmpty(currentSchema))
				throw new ArgumentNullException("currentSchema");
			if (transaction == null)
				throw new ArgumentNullException("transaction");

			CurrentSchema =currentSchema;
			Transaction = transaction;
		    Context = transaction.Context.CreateSessionContext();
			Context.RegisterInstance(this);

			User = new User(this, User.SystemName);

			startedOn = DateTimeOffset.UtcNow;

			Access = new SessionAccess(this);
		}

		~SystemSession() {
			Dispose(false);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (Context != null)
					Context.Dispose();
			}

			Context = null;
			Transaction = null;
		}

		public string CurrentSchema { get; private set; }

		private SessionAccess Access { get; set; }

		SystemAccess IProvidesDirectAccess.DirectAccess {
			get { return Access; }
		}

		public User User { get; private set; }

		public ITransaction Transaction { get; private set; }

        public ISessionContext Context { get; private set; }

		IContext IContextBased.Context {
			get { return Context; }
		}

		public ILargeObject CreateLargeObject(long size, bool compressed) {
			throw new NotSupportedException();
		}

		public ILargeObject GetLargeObject(ObjectId objId) {
			throw new NotSupportedException();
		}

		public void SetTimeZone(int hours, int minutes) {
		}

		public void Commit() {
			Transaction.Commit();
		}

		public void Rollback() {
			Transaction.Rollback();
		}

		public IQuery CreateQuery() {
			return new Query(this);
		}
	}
}