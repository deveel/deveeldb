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
using System.Collections.Generic;
using System.IO;

using Deveel.Data.DbSystem;
using Deveel.Data.Routines;
using Deveel.Data.Threading;
using Deveel.Diagnostics;

namespace Deveel.Data.Protocol {
	public class EmbeddedServerConnector : EmbeddedServerConnectorBase {
		public EmbeddedServerConnector(IDatabase database)
			: base(database) {
		}

		protected override IQueryResponse[] ExecuteQuery(string text, IEnumerable<SqlQueryParameter> parameters) {
			// Log this Query if Query logging is enabled
			if (Logger.IsInterestedIn(LogLevel.Debug)) {
				// Output the instruction to the _queries log.
				Logger.DebugFormat(this, "[CLIENT] [{0}] - Query: {1}", Session.User.UserName, text);
			}

			// Write debug message (Info level)
			if (Logger.IsInterestedIn(LogLevel.Debug)) {
				Logger.DebugFormat(this, "Query From User: {0}", Session.User.UserName);
				Logger.DebugFormat(this, "Query: {0}", text.Trim());
			}

			// Get the locking mechanism.
			LockingMechanism locker = Session.Connection.LockingMechanism;
			LockingMode lockMode = LockingMode.None;
			IQueryResponse[] response = null;
			try {
				try {
					// For simplicity - all database locking is now exclusive inside
					// a transaction.  This means it is not possible to execute
					// queries concurrently inside a transaction.  However, we are
					// still able to execute queries concurrently from different
					// connections.
					//
					// It's debatable whether we even need to perform this Lock anymore
					// because we could change the contract of this method so that
					// it is not thread safe.  This would require that the callee ensures
					// more than one thread can not execute queries on the connection.
					lockMode = LockingMode.Exclusive;
					locker.SetMode(lockMode);

					// Execute the Query (behaviour for this comes from super).
					response = base.ExecuteQuery(text, parameters);

					// Return the result.
					return response;

				} finally {
					try {
						// This is executed no matter what happens.  Very important we
						// unlock the tables.
						if (lockMode != LockingMode.None) {
							locker.FinishMode(lockMode);
						}
					} catch (Exception e) {
						// If this throws an exception, we should output it to the debug
						// log and screen.
						Logger.Error(this, "Exception finishing locks");
						Logger.Error(this, e);
						// Note, we can't throw an error here because we may already be in
						// an exception that happened in the above 'try' block.
					}
				}
			} finally {
				// This always happens after tables are unlocked.
				// Also guarenteed to happen even if something fails.

				// If we are in auto-commit mode then commit the Query here.
				// Do we auto-commit?
				if (Session.Connection.AutoCommit) {
					// Yes, so grab an exclusive Lock and auto-commit.
					try {
						// Lock into exclusive mode.
						locker.SetMode(LockingMode.Exclusive);
						// If an error occured then roll-back
						if (response == null) {
							// Rollback.
							Session.Connection.Rollback();
						} else {
							try {
								// Otherwise commit.
								Session.Connection.Commit();
							} catch (Exception e) {
								foreach (IQueryResponse queryResponse in response) {
									// Dispose this response if the commit failed.
									DisposeResult(queryResponse.ResultId);
								}

								// And throw the SQL Exception
								throw;
							}
						}
					} finally {
						locker.FinishMode(LockingMode.Exclusive);
					}
				}
			}
		}
	}
}