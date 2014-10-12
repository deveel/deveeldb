// 
//  Copyright 2010-2014 Deveel
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

using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Security;
using Deveel.Data.Threading;

namespace Deveel.Data.Control {
	public sealed class AuthenticatedSession : IDisposable {
		public AuthenticatedSession(User user, IDatabaseConnection connection) {
			if (user == null)
				throw new ArgumentNullException("user");
			if (connection == null)
				throw new ArgumentNullException("connection");

			User = user;
			Connection = connection;
		}

		~AuthenticatedSession() {
			Dispose(false);
		}

		public User User { get; private set; }

		public IDatabaseConnection Connection { get; private set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (Connection != null) {
					LockingMechanism locker = Connection.LockingMechanism;
					try {
						// Lock into exclusive mode,
						locker.SetMode(LockingMode.Exclusive);
						// Roll back any open transaction.
						Connection.Rollback();
					} finally {
						// Finish being in exclusive mode.
						locker.FinishMode(LockingMode.Exclusive);
						// Close the database connection object.
						Connection.Close();
						// Log out the user
						User.Logout();
					}
					
					Connection.Dispose();
				}
			}
		}
	}
}