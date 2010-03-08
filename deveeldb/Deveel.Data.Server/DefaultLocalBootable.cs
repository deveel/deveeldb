// 
//  Copyright 2010  Deveel
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

using Deveel.Data.Client;
using Deveel.Data.Control;

namespace Deveel.Data.Server {
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
		private bool booted = false;

        /// <summary>
        /// Set to true when this interface is active.
        /// </summary>
		private bool active = false;

        /// <summary>
        /// The local DbSystem database object.
        /// </summary>
		private DbSystem dbsys;

        /// <summary>
        /// The connection id.
        /// </summary>
        /// <remarks>
        /// This is incremented by 1 each time an interface connects to the 
        /// local runtime.
        /// </remarks>
		private int connect_id = 0;

        /// <summary>
        /// The number of connections that are current open.
        /// </summary>
		private int open_connections = 0;

        /// <summary>
        /// The connection Lock object.
        /// </summary>
		private Object connection_lock = new Object();

        /// <inheritdoc/>
		public IDatabaseInterface Create(string username, String password, IDbConfig config) {
			if ((username == null || username.Equals("")) || 
				(password == null || password.Equals("")))
				throw new DataException("Username and Password must both be set.");

			if (!booted) {
				// Local connections are formatted as;
				// 'Local/[type]/[connect_id]'
				const string host_string = "Local/Create/";

				// Create the DbSystem and bind it to a IDatabaseInterface.
				dbsys = controller.CreateDatabase(config, databaseName, username, password);
				IDatabaseInterface db_interface = new LocalDatabaseInterface(this, host_string);

				booted = true;
				++open_connections;
				active = true;

				return db_interface;
			}

			throw new DataException("Database is already created.");
		}


		Database IDatabaseHandler.GetDatabase(string name) {
			return (dbsys == null ? null : dbsys.Database);
		}

        /// <inheritdoc/>
		public IDatabaseInterface Boot(IDbConfig config) {
			if (!booted) {
				// Local connections are formatted as;
				// 'Local/[type]/[connect_id]'
				const string host_string = "Local/Boot/";

				// Start the DbSystem and bind it to a IDatabaseInterface.
				if (controller.IsInitialized(databaseName))
					dbsys = controller.ConnectToDatabase(databaseName);
				else
					dbsys = controller.StartDatabase(config, databaseName);

				IDatabaseInterface db_interface = new LocalDatabaseInterface(this, host_string);

				booted = true;
				++open_connections;
				active = true;

				return db_interface;

			}
			
			throw new DataException("Database was booted more than once.");
		}

        /// <inheritdoc/>
		public bool CheckExists() {
			if (!booted) {
				return controller.DatabaseExists(databaseName);
			} else {
				throw new DataException("The database is already booted.");
			}
		}

        /// <inheritdoc/>
	    public bool IsBooted {
	        get { return booted; }
	    }

        /// <inheritdoc/>
		public IDatabaseInterface Connect() {
			if (booted) {

				// Local connections are formatted as;
				// 'Local/[type]/[connect_id]'
				String host_string = "Local/Connection/" + connect_id;

				// Create a IDatabaseInterface,
				IDatabaseInterface db_interface = new LocalDatabaseInterface(this, host_string);

				++connect_id;
				++open_connections;
				active = true;

				return db_interface;

			} else {
				throw new DataException("The database is not started.");
			}

		}

		// ---------- Inner classes ----------

        /// <summary>
        /// A local implementation of DatabaseInterface that will 
        /// dispose the parent <see cref="ILocalBootable"/> object when 
        /// the last open connection is disposed.
        /// </summary>
		private class LocalDatabaseInterface : DatabaseInterface {
			private readonly DefaultLocalBootable local_bootable;
			bool closed;

			public LocalDatabaseInterface(DefaultLocalBootable local_bootable, String host_string)
				: base(local_bootable, local_bootable.databaseName, host_string) {
				this.local_bootable = local_bootable;
			}

			// ---------- Overwritten from DatabaseInterface ----------

			protected override void Dispose() {
				if (!closed) {
					base.Dispose();

					--local_bootable.open_connections;

					// When all connections are closed, shut down...
					if (local_bootable.open_connections <= 0) {
						// When the local database interface is disposed, we must shut down
						// the database system.
						local_bootable.dbsys.Close();
						local_bootable.active = false;
						local_bootable.booted = false;
						local_bootable.dbsys = null;
					}
					closed = true;
				}

			}
		}
	}
}