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
using System.Transactions;

namespace Deveel.Data.Client {
	class DeveelDbEnlistment : IEnlistmentNotification {
		private DeveelDbTransaction transaction;

		public DeveelDbEnlistment(DeveelDbConnection connection, Transaction scope) {
			transaction = connection.BeginTransaction();

			Scope = scope;
			Scope.EnlistVolatile(this, EnlistmentOptions.None);
		}

		public Transaction Scope { get; private set; }

		public void Prepare(PreparingEnlistment preparingEnlistment) {
			DeveelDbException error;
			if (!transaction.IsOpen(out error)) {
				preparingEnlistment.ForceRollback(error);
			} else {
				preparingEnlistment.Prepared();
			}
		}

		public void Commit(Enlistment enlistment) {
			var connection = transaction.Connection;

			try {
				transaction.AssertOpen();
				transaction.Commit();
				enlistment.Done();
			} finally {
				Dispose(connection);
			}
		}

		private void Dispose(DeveelDbConnection connection) {
			connection.Dispose();
			transaction = null;
			Scope = null;
		}

		public void Rollback(Enlistment enlistment) {
			var connection = transaction.Connection;

			try {
				transaction.Rollback();
				enlistment.Done();
			} finally {
				Dispose(connection);
			}
		}

		public void InDoubt(Enlistment enlistment) {
			enlistment.Done();
		}
	}
}
