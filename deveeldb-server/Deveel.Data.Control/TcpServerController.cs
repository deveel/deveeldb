using System;
using System.Collections;
using System.IO;
using System.Net;
using System.Net.Sockets;

using Deveel.Data.Server;

namespace Deveel.Data.Control {
	/// <summary>
	/// Attaches to a <see cref="DbSystem"/>, and binds a TCP port and serves 
	/// queries for connections.
	/// </summary>
	/// <remarks>
	/// This object is used to programmatically create a TCP server on the local 
	/// machine.
	/// <para>
	/// Note that multiple servers can be constructed to serve the same <see cref="DbSystem"/>. 
	/// This object cannot be used to connect a single TCP server to multiple 
	/// <see cref="DbSystem"/> objects.
	/// </para>
	/// <para>
	/// If the underlying database is shut down then this server is also shut down.
	/// </para>
	/// </remarks>
	public class TcpServerController {
		/// <summary>
		/// The default TCP port for DeveelDB SQL Database.
		/// </summary>
		internal const int DefaultPort = 9157;	//TODO: change this value...

		private IDbConfig config;

		/// <summary>
		/// An <see cref="IPAddress"/> representing the interface that server is 
		/// bound to - useful for multi-homed machines.
		/// </summary>
		/// <remarks>
		/// A <b>null</b> value means we bind to all interfaces.
		/// </remarks>
		private readonly IPAddress bind_address;

		/// <summary>
		/// The TCP port that this server is bound to.
		/// </summary>
		private readonly int tcp_port;

		/// <summary>
		/// The <see cref="TcpServer"/> object that is managing the connections to 
		/// this database.
		/// </summary>
		private TcpServer server;

		private readonly Hashtable databases;

		/// <summary>
		/// Constructs the TCP Server with the given <see cref="DbSystem"/> object, 
		/// and sets the IP address and TCP port that we serve the database from.
		/// </summary>
		/// <param name="system"></param>
		/// <param name="bind_address"></param>
		/// <param name="tcp_port"></param>
		/// <remarks>
		/// Constructing this server does not open the port to receive connections 
		/// from outside. To start the server it is needed a call the <see cref="Start"/>
		/// method.
		/// </remarks>
		public TcpServerController(IDbConfig config, IPAddress bind_address, int tcp_port) {
			this.config = config;
			this.bind_address = bind_address;
			this.tcp_port = tcp_port;
			databases = new Hashtable();
			RegisterShutdownDelegate();
		}

		/// <summary>
		/// Constructs the TCP Server with the given <see cref="DbSystem"/> object, 
		/// and sets the IP address and TCP port that we serve the database from,
		/// binding the server to all interfaces on the local machine.
		/// </summary>
		/// <param name="system"></param>
		/// <param name="tcp_port"></param>
		/// <remarks>
		/// Constructing this server does not open the port to receive connections 
		/// from outside. To start the server it is needed a call the <see cref="Start"/>
		/// method.
		/// </remarks>
		public TcpServerController(IDbConfig config, int tcp_port)
			: this(config, null, tcp_port) {
		}

		/// <summary>
		/// Constructs the TCP Server with the given <see cref="DbSystem"/> object, 
		/// and sets the TCP port and address (for multi-homed computers) to the 
		/// setting of the configuration in <paramref name="system"/>.
		/// </summary>
		/// <param name="system"></param>
		/// <remarks>
		/// Constructing this server does not open the port to receive connections 
		/// from outside. To start the server it is needed a call the <see cref="Start"/>
		/// method.
		/// </remarks>
		public TcpServerController(IDbConfig config) {
			this.config = config;
			databases = new Hashtable();

			string root_path = GetRootPath(config.CurrentPath);
			RetrieveDatabases(root_path);

			int port = DefaultPort;
			IPAddress interface_address;

			// Read the config properties.
			String port_str = config.GetValue("server_port");
			String interface_addr_str = config.GetValue("server_address");

			if (port_str != null) {
				try {
					port = Int32.Parse(port_str);
				} catch (Exception e) {
					throw new ApplicationException("Unable to parse 'server_port'");
				}
			}
			if (interface_addr_str != null) {
				try {
					interface_address = IPAddress.Parse(interface_addr_str);
				} catch (SocketException e) {
					throw new ApplicationException("Unknown host: " + e.Message);
				}
			} else {
				interface_address = IPAddress.Parse("127.0.0.1");
			}

			// Set up this port and bind address
			tcp_port = port;
			bind_address = interface_address;

			RegisterShutdownDelegate();
		}

		private string GetRootPath(string basePath) {
			string root_path = config.GetValue("root_path");
			if (root_path == null || root_path == "env")
				return basePath;
			return Path.Combine(basePath, root_path);
		}

		private void RetrieveDatabases(string rootPath) {
			string configFile = config.GetValue("config_file");
			if (configFile == null)
				configFile = "db.conf";

			DbController controller = DbController.Default;
			string[] sub_dirs = Directory.GetDirectories(rootPath);
			for (int i = 0; i < sub_dirs.Length; i++) {
				string dir_name = sub_dirs[i];
				configFile = Path.Combine(dir_name, configFile);
				DbConfig dbConfig;
				if (File.Exists(configFile)) {
					dbConfig = new DbConfig(dir_name);
					dbConfig.LoadFromFile(configFile);
				} else {
					dbConfig = new DefaultDbConfig(dir_name);
				}

				IDbConfig defConfig = (IDbConfig)config.Clone();
				defConfig.Merge(dbConfig);

				string dbName = defConfig.GetValue("name");
				if (dbName == null)
					dbName = Path.GetDirectoryName(dir_name);

				DbSystem dbSystem = controller.StartDatabase(dbConfig, dbName);
				databases[dbName] = dbSystem.Database;
			}
		}

		/// <summary>
		/// Registers the delegate that closes this server when the database shuts-down.
		/// </summary>
		private void RegisterShutdownDelegate() {
			foreach (Database database in databases.Values) {
				database.RegisterShutDownDelegate(new StopEventImpl(this, database));
			}
		}

		private void OnDatabaseShutdown(Database database) {
			if (server != null) {
				databases.Remove(database.Name);
				if (databases.Count == 0)
					Stop();
			}
		}

		private class StopEventImpl : IDatabaseEvent {
			public StopEventImpl(TcpServerController controller, Database database) {
				this.controller = controller;
				this.database = database;
			}

			private readonly Database database;
			private readonly TcpServerController controller;

			public void Execute() {
				controller.OnDatabaseShutdown(database);
			}
		}

		/// <summary>
		/// Starts the server and binds it to the given port.
		/// </summary>
		/// <remarks>
		/// This method will start a new thread that listens for incoming 
		/// connections.
		/// </remarks>
		public void Start() {
			lock (this) {
				if (server == null) {
					server = new TcpServer(this);
					server.Start(bind_address, tcp_port, ConnectionPoolModel.MultiThreaded);
				} else {
					throw new ApplicationException("'Start' method called when a server was already started.");
				}
			}
		}

		/// <summary>
		/// Stops the server running on the given port.
		/// </summary>
		/// <remarks>
		/// This method will stop any threads that are listening for incoming 
		/// connections.
		/// <para>
		/// Note that this does <b>not</b> Close the underlying <see cref="DbSystem"/> 
		/// object: it must be closed separately.
		/// </para>
		/// </remarks>
		public void Stop() {
			lock (this) {
				if (server != null) {
					server.Close();
					server = null;
				} else {
					throw new ApplicationException("'Stop' method called when no server was started.");
				}
			}
		}

		/// <summary>
		/// Gets the <see cref="DbSystem"/> associated with the
		/// given database name.
		/// </summary>
		/// <param name="name">The name of the database.</param>
		/// <returns>
		/// Retuns a <see cref="DbSystem"/> instance that is associated with the
		/// name of the database specified.
		/// </returns>
		public Database GetDatabase(string name) {
			return databases[name] as Database;
		}

		public void Execute(IDatabaseEvent dbEvent) {
			foreach (Database database in databases.Values) {
				database.Execute(null, null, dbEvent);
			}
		}

		/// <summary>
		/// Returns a string that contains some information about the server 
		/// that is running.
		/// </summary>
		/// <returns></returns>
		public override String ToString() {
			return server.ToString();
		}
	}
}