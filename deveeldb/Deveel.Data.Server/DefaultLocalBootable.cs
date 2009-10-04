//  
//  DefaultLocalBootable.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
	public class DefaultLocalBootable : ILocalBootable {
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
		public IDatabaseInterface Create(String username, String password, IDbConfig config) {
			if (username.Equals("") || password.Equals("")) {
				throw new DataException("Username and Password must both be set.");
			}

			if (!booted) {
				// Local connections are formatted as;
				// 'Local/[type]/[connect_id]'
				String host_string = "Local/Create/";

				// Create the DbSystem and bind it to a IDatabaseInterface.
				DbController controller = DbController.Default;
				dbsys = controller.CreateDatabase(config, username, password);
				IDatabaseInterface db_interface =
				   new LocalJDBCDatabaseInterface(this, dbsys.Database, host_string);

				booted = true;
				++open_connections;
				active = true;

				return db_interface;
			}

			throw new DataException("Database is already created.");

		}

        /// <inheritdoc/>
		public IDatabaseInterface Boot(IDbConfig config) {
			if (!booted) {
				// Local connections are formatted as;
				// 'Local/[type]/[connect_id]'
				String host_string = "Local/Boot/";

				// Start the DbSystem and bind it to a IDatabaseInterface.
				DbController controller = DbController.Default;
				dbsys = controller.StartDatabase(config);
				IDatabaseInterface db_interface =
				   new LocalJDBCDatabaseInterface(this, dbsys.Database, host_string);

				booted = true;
				++open_connections;
				active = true;

				return db_interface;

			} else {
				throw new DataException("Database was booted more than once.");
			}
		}

        /// <inheritdoc/>
		public bool CheckExists(IDbConfig config) {
			if (!booted) {
				DbController controller = DbController.Default;
				return controller.DatabaseExists(config);
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
				IDatabaseInterface db_interface =
					new LocalJDBCDatabaseInterface(this, dbsys.Database, host_string);

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
		private class LocalJDBCDatabaseInterface : DatabaseInterface {
			private readonly DefaultLocalBootable local_bootable;
			bool closed;

			public LocalJDBCDatabaseInterface(DefaultLocalBootable local_bootable, Database database, String host_string)
				: base(database, host_string) {
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