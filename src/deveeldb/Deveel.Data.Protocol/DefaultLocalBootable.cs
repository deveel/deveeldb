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
using System.Data;

using Deveel.Data.Configuration;
using Deveel.Data.Control;
using Deveel.Data.DbSystem;

namespace Deveel.Data.Protocol {
	///<summary>
	/// A bootable object that filters through to a <see cref="DatabaseInterface"/> 
	/// but is thread-safe and multi-threaded. 
	///</summary>
	/// <remarks>
	/// This is to be used when you have a local client accessing a stand-alone database.
	/// </remarks>
	public class DefaultLocalBootable : ILocalBootable, IDatabaseHandler {
		public DefaultLocalBootable(DbController controller, string databaseName) {
			this.controller = controller;
			this.databaseName = databaseName;
		}

		private readonly DbController controller;
		private readonly string databaseName;

        /// <summary>
        /// Set to true if the database is booted.
        /// </summary>
		private bool booted;

        /// <summary>
        /// The local DbSystem database object.
        /// </summary>
		private Control.DbSystem dbsys;

        /// <summary>
        /// The connection id.
        /// </summary>
        /// <remarks>
        /// This is incremented by 1 each time an interface connects to the 
        /// local runtime.
        /// </remarks>
		private int connectId;

        /// <summary>
        /// The number of connections that are current open.
        /// </summary>
		private int openConnections;

        /// <inheritdoc/>
		public IDatabaseInterface Create(string username, string password, IDbConfig config) {
			if (String.IsNullOrEmpty(username) || 
				String.IsNullOrEmpty(password))
				throw new DataException("Username and Password must both be set.");

        	if (booted)
        		throw new DataException("Database is already created.");

        	// Local connections are formatted as;
        	// 'Local/[type]/[connect_id]'
        	string hostString = String.Format("{0}/Create/", KnownConnectionProtocols.Local);

        	// Create the DbSystem and bind it to a IDatabaseInterface.
        	dbsys = controller.CreateDatabase(config, databaseName, username, password);
        	IDatabaseInterface dbInterface = new LocalDatabaseInterface(this, hostString);

        	booted = true;
        	++openConnections;

        	return dbInterface;
        }


		IDatabase IDatabaseHandler.GetDatabase(string name) {
			return (dbsys == null ? null : dbsys.Database);
		}

        /// <inheritdoc/>
		public IDatabaseInterface Boot(IDbConfig config) {
        	if (booted)
        		throw new DataException("Database was booted more than once.");

        	// Local connections are formatted as;
        	// 'Local/[type]/[connect_id]'
        	string hostString = String.Format("{0}/Boot/", KnownConnectionProtocols.Local);

        	// Start the DbSystem and bind it to a IDatabaseInterface.
        	if (controller.IsInitialized(databaseName))
        		dbsys = controller.ConnectToDatabase(config, databaseName);
        	else
        		dbsys = controller.StartDatabase(config, databaseName);

        	IDatabaseInterface dbInterface = new LocalDatabaseInterface(this, hostString);

        	booted = true;
        	++openConnections;

        	return dbInterface;
        }

        /// <inheritdoc/>
		public bool CheckExists(IDbConfig config) {
        	if (booted)
        		throw new DataException("The database is already booted.");

        	return controller.DatabaseExists(config, databaseName);
        }

		/// <inheritdoc/>
	    public bool IsBooted {
	        get { return booted; }
	    }

        /// <inheritdoc/>
		public IDatabaseInterface Connect() {
        	if (!booted)
        		throw new DataException("The database is not started.");

        	// Local connections are formatted as;
        	// 'Local/[type]/[connect_id]'
        	string hostString = String.Format("{0}/Connection/{1}", KnownConnectionProtocols.Local, connectId);

        	// Create a IDatabaseInterface,
        	IDatabaseInterface dbInterface = new LocalDatabaseInterface(this, hostString);

        	++connectId;
        	++openConnections;

        	return dbInterface;
        }

		// ---------- Inner classes ----------

        /// <summary>
        /// A local implementation of DatabaseInterface that will 
        /// dispose the parent <see cref="ILocalBootable"/> object when 
        /// the last open connection is disposed.
        /// </summary>
		private class LocalDatabaseInterface : DatabaseInterface {
			private readonly DefaultLocalBootable localBootable;
        	private bool closed;

			public LocalDatabaseInterface(DefaultLocalBootable localBootable, string hostString)
				: base(localBootable, localBootable.databaseName, hostString) {
				this.localBootable = localBootable;
			}

			// ---------- Overwritten from DatabaseInterface ----------

			protected override void Dispose(bool disposing) {
				if (disposing) {
					if (!closed) {
						base.Dispose(true);

						--localBootable.openConnections;

						// When all connections are closed, shut down...
						if (localBootable.openConnections <= 0) {
							// When the local database interface is disposed, we must shut down
							// the database system.
							localBootable.dbsys.Close();
							localBootable.booted = false;
							localBootable.dbsys = null;
						}
						closed = true;
					}
				}
			}
		}
	}
}