//  
//  DbConnection.cs
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
using System.Collections;
using System.Data;
using System.IO;
using System.Net;
using System.Threading;

using Deveel.Data.Control;
using Deveel.Data.Server;
using Deveel.Data.Util;
using Deveel.Math;

namespace Deveel.Data.Client {
	///<summary>
	/// Implementation of the <see cref="IDbConnection">connection</see> object 
	/// to a database.
	///</summary>
	/// <remarks>
	/// The implementation specifics for how the connection talks with the database
	/// is left up to the implementation of <see cref="IDatabaseInterface"/>.
	/// <para>
	/// This object is thread safe. It may be accessed safely from concurrent threads.
	/// </para>
	/// </remarks>
	public class DbConnection : IDbConnection, IDatabaseCallBack {
		/// <summary>
		/// The mapping of the database configuration URL string to the 
		/// <see cref="ILocalBootable"/> object that manages the connection.
		/// </summary>
		/// <remarks>
		/// This mapping is only used if the driver makes local connections (eg. 'local://').
		/// </remarks>
		private readonly Hashtable local_session_map = new Hashtable();

		/// <summary>
		/// A cache of all rows retrieved from the server.
		/// </summary>
		/// <remarks>
		/// This cuts down the number of requests to the server by caching rows that 
		/// are accessed frequently.  Note that cells are only cached within a ResultSet 
		/// bounds. Two different ResultSet's will not share cells in the cache.
		/// </remarks>
		private readonly RowCache row_cache;

		/// <summary>
		/// The URL used to make this connection.
		/// </summary>
		private readonly String url;

		/// <summary>
		/// Set to true if the connection is closed.
		/// </summary>
		private bool is_closed;

		/// <summary>
		/// Set to true if the connection is in auto-commit mode.
		/// (By default, auto_commit is enabled).
		/// </summary>
		private bool auto_commit;

		/// <summary>
		/// The interface to the database.
		/// </summary>
		private readonly IDatabaseInterface db_interface;

		/// <summary>
		/// The list of trigger listeners registered with the connection.
		/// </summary>
		private readonly ArrayList trigger_list;

		/// <summary>
		/// A Thread that handles all dispatching of trigger events to the client.
		/// </summary>
		private TriggerDispatchThread trigger_thread;

		/// <summary>
		/// If the <see cref="DbDataReader.GetValue"/> method should return the 
		/// raw object type (eg. <see cref="BigDecimal"/> for integer, <see cref="String"/> 
		/// for chars, etc) then this is set to false.
		/// If this is true (the default) the <see cref="DbDataReader.GetValue"/> methods 
		/// return the correct object types as specified by the ADO.NET specification.
		/// </summary>
		private bool strict_get_object;

		/// <summary>
		/// If the <see cref="DbDataReader.GetName"/> method should return a succinct 
		/// form of the column name as most implementations do, this should be set to 
		/// false (the default).
		/// </summary>
		/// <remarks>
		/// If old style verbose column names should be returned for compatibility with 
		/// older versions, this is set to true.
		/// </remarks>
		private bool verbose_column_names;

		/// <summary>
		/// This is set to true if the ResultSet column lookup methods are case
		/// insensitive.
		/// </summary>
		/// <remarks>
		/// This should be set to true for any database that has case insensitive 
		/// identifiers.
		/// </remarks>
		private bool case_insensitive_identifiers;

		/// <summary>
		/// A mapping from a streamable object id to <see cref="Stream"/> used to 
		/// represent the object when being uploaded to the database engine.
		/// </summary>
		private readonly Hashtable s_object_hold;

		/// <summary>
		/// An unique id count given to streamable object being uploaded to the server.
		/// </summary>
		private long s_object_id;



		// For synchronization in this object,
		private readonly Object @lock = new Object();

		public DbConnection(String url, IDatabaseInterface db_interface, int cache_size, int max_size) {
			this.url = url;
			this.db_interface = db_interface;
			is_closed = true;
			auto_commit = true;
			trigger_list = new ArrayList();
			strict_get_object = true;
			verbose_column_names = false;
			case_insensitive_identifiers = false;
			row_cache = new RowCache(cache_size, max_size);
			s_object_hold = new Hashtable();
			s_object_id = 0;
		}

		public DbConnection(string url, Properties info) {
			IDatabaseInterface db_interface;
			String default_schema = "APP";

			int row_cache_size;
			int max_row_cache_size;

			String address_part = url;
			// If we are to connect to a single user database running
			// within this runtime.
			if (address_part.StartsWith("local://")) {

				// Returns a list of two Objects, db_interface and database_name.
				Object[] ret_list = ConnectToLocal(address_part, info);
				db_interface = (IDatabaseInterface)ret_list[0];
				default_schema = (String)ret_list[1];

				// Internal row cache setting are set small.
				row_cache_size = 43;
				max_row_cache_size = 4092000;

			} else {
				int port = 9157;
				String host = "127.0.0.1";

				// Otherwise we must be connecting remotely.
				if (address_part.StartsWith("//")) {

					String args_string = "";
					int arg_part = address_part.IndexOf('?', 2);
					if (arg_part != -1) {
						args_string = address_part.Substring(arg_part + 1);
						address_part = address_part.Substring(0, arg_part);
					}

					//        Console.Out.WriteLine("ADDRESS_PART: " + address_part);

					int end_address = address_part.IndexOf("/", 2);
					if (end_address == -1) {
						end_address = address_part.Length;
					}
					String remote_address = address_part.Substring(2, end_address);
					int delim = remote_address.IndexOf(':');
					if (delim == -1) {
						delim = remote_address.Length;
					}
					host = remote_address.Substring(0, delim);
					if (delim < remote_address.Length - 1) {
						port = Int32.Parse(remote_address.Substring(delim + 1));
					}

					//        Console.Out.WriteLine("REMOTE_ADDRESS: '" + remote_address + "'");

					// Schema name?
					String schema_part = "";
					if (end_address < address_part.Length) {
						schema_part = address_part.Substring(end_address + 1);
					}
					String schema_string = schema_part;
					int schema_end = schema_part.IndexOf('/');
					if (schema_end != -1) {
						schema_string = schema_part.Substring(0, schema_end);
					} else {
						schema_end = schema_part.IndexOf('?');
						if (schema_end != -1) {
							schema_string = schema_part.Substring(0, schema_end);
						}
					}

					//        Console.Out.WriteLine("SCHEMA_STRING: '" + schema_string + "'");

					// Argument part?
					if (!args_string.Equals("")) {
						//          Console.Out.WriteLine("ARGS: '" + args_string + "'");
						ParseEncodedVariables(args_string, info);
					}

					// Is there a schema or should we default?
					if (schema_string.Length > 0) {
						default_schema = schema_string;
					}

				} else {
					if (address_part.Trim().Length > 0) {
						throw new DataException("Malformed URL: " + address_part);
					}
				}

				//      database_name = address_part;
				//      if (database_name == null || database_name.trim().equals("")) {
				//        database_name = "DefaultDatabase";
				//      }

				// BUG WORKAROUND:
				// There appears to be a bug in the socket code of some VM
				// implementations.  With the IBM Linux JDK, if a socket is opened while
				// another is closed while blocking on a Read, the socket that was just
				// opened breaks.  This was causing the login code to block indefinitely
				// and the connection thread causing a null pointer exception.
				// The workaround is to WriteByte a short pause before the socket connection
				// is established.
				try {
					Thread.Sleep(85);
				} catch (ThreadInterruptedException) { /* ignore */ }

				// Make the connection
				TCPStreamDatabaseInterface tcp_db_interface =
											 new TCPStreamDatabaseInterface(host, port);
				// Attempt to open a socket to the database.
				tcp_db_interface.ConnectToDatabase();

				db_interface = tcp_db_interface;

				// For remote connection, row cache uses more memory.
				row_cache_size = 4111;
				max_row_cache_size = 8192000;

			}

			this.url = url;
			this.db_interface = db_interface;
			is_closed = true;
			auto_commit = true;
			trigger_list = new ArrayList();
			strict_get_object = true;
			verbose_column_names = false;
			case_insensitive_identifiers = false;
			row_cache = new RowCache(row_cache_size, max_row_cache_size);
			s_object_hold = new Hashtable();
			s_object_id = 0;

			Login(info, default_schema);
		}

		/// <summary>
		/// Makes a connection to a local database.
		/// </summary>
		/// <param name="address_part"></param>
		/// <param name="info"></param>
		/// <remarks>
		/// If a local database connection has not been made then it is created here.
		/// </remarks>
		/// <returns>
		/// Returns a list of two elements, (<see cref="IDatabaseInterface"/>) db_interface 
		/// and (<see cref="String"/>) database_name.
		/// </returns>
		private object[] ConnectToLocal(string address_part, Properties info) {
			lock (this) {
				// If the ILocalBootable object hasn't been created yet, do so now via
				// reflection.
				String schema_name = "APP";
				IDatabaseInterface db_interface;

				// Look for the name upto the URL encoded variables
				int url_start = address_part.IndexOf("?");
				if (url_start == -1) {
					url_start = address_part.Length;
				}

				// The path to the configuration
				String config_path = address_part.Substring(8, url_start - 8);

				// If no config_path, then assume it is ./db.conf
				if (config_path.Length == 0) {
					config_path = "./db.conf";
				}

				// Substitute win32 '\' to unix style '/'
				config_path = config_path.Replace('\\', '/');

				// Is the config path encoded as a URL?
				if (config_path.StartsWith("file:/") ||
					config_path.StartsWith("ftp:/") ||
					config_path.StartsWith("http:/") ||
					config_path.StartsWith("https:/")) {
					// Don't do anything - looks like a URL already.
				} else {
					// We don't care about anything after the ".conf/"
					String abs_path;
					String post_abs_path;
					int schem_del = config_path.IndexOf(".conf/");
					if (schem_del == -1) {
						abs_path = config_path;
						post_abs_path = "";
					} else {
						abs_path = config_path.Substring(0, schem_del + 5);
						post_abs_path = config_path.Substring(schem_del + 5);
					}

					// If the config path is not encoded as a URL, add a 'file:/' preffix
					// to the path to make it a URL.  For example 'C:/my_config.conf" becomes
					// 'file:/C:/my_config.conf'

					String path_part = abs_path;
					String rest_part = "";
					String pre = "file:/";

					// Does the configuration file exist?  Or does the resource that contains
					// the configuration exist?
					// We try the file with a preceeding '/' and without.
					string f = Path.GetFullPath(path_part);
					if (!File.Exists(f) && !path_part.StartsWith("/")) {
						f = "/" + path_part;
						if (!File.Exists(f)) {
							throw new DataException("Unable to find file: " + path_part);
						}
					}
					// Construct the new qualified configuration path.
					config_path = pre + Path.GetFullPath(f) + rest_part + post_abs_path;
					// Substitute win32 '\' to unix style '/'
					// We do this (again) because on win32 'Path.GetFullPath(f)' returns win32
					// style deliminators.
					config_path = config_path.Replace('\\', '/');
				}

				// Look for the string '.conf/' in the config_path which is used to
				// determine the initial schema name.  For example, the connection URL,
				// 'local:///my_db/db.conf/ANTO' will start the database in the
				// ANTO schema of the database denoted by the configuration path
				// '/my_db/db.conf'
				int schema_del_i = config_path.ToLower().IndexOf(".conf/");
				if (schema_del_i > 0 &&
					schema_del_i + 6 < config_path.Length) {
					schema_name = config_path.Substring(schema_del_i + 6);
					config_path = config_path.Substring(0, schema_del_i + 5);
				}

				// The url variables part
				String url_vars = "";
				if (url_start < address_part.Length) {
					url_vars = address_part.Substring(url_start + 1).Trim();
				}

				// Is there already a local connection to this database?
				String session_key = config_path.ToLower();
				ILocalBootable local_bootable =
					(ILocalBootable)local_session_map[session_key];
				// No so create one and WriteByte it in the connection mapping
				if (local_bootable == null) {
					local_bootable = CreateDefaultLocalBootable();
					local_session_map[session_key] = local_bootable;
				}

				// Is the connection booted already?
				if (local_bootable.IsBooted) {
					// Yes, so simply login.
					db_interface = local_bootable.Connect();
				} else {
					// Otherwise we need to boot the local database.

					// This will be the configuration input file
					Stream config_in;
					if (!config_path.StartsWith("file:/")) {
						// Make the config_path into a URL and open an input stream to it.
						Uri config_url;
						try {
							config_url = new Uri(config_path);
						} catch (FormatException) {
							throw new DataException("Malformed URL: " + config_path);
						}

						try {
							// Try and open an input stream to the given configuration.
							WebRequest request = WebRequest.Create(config_url);
							WebResponse response = request.GetResponse();
							config_in = response.GetResponseStream();
						} catch (IOException) {
							throw new DataException("Unable to open configuration file.  " +
												   "I tried looking at '" + config_url + "'");
						}
					} else {
						try {
							// Try and open an input stream to the given configuration.
							config_in = new FileStream(config_path.Substring(6), FileMode.Open, FileAccess.ReadWrite);
						} catch (IOException) {
							throw new DataException("Unable to open configuration file: " + config_path);
						}

					}

					// Work out the root path (the place in the local file system where the
					// configuration file is).
					string root_path;
					// If the URL is a file, we can work out what the root path is.
					if (config_path.StartsWith("file:/")) {

						int start_i = config_path.IndexOf(":/");

						// If the config_path is pointing inside a jar file, this denotes the
						// end of the file part.
						int file_end_i = config_path.IndexOf("!");
						String config_file_part;
						if (file_end_i == -1) {
							config_file_part = config_path.Substring(start_i + 2);
						} else {
							config_file_part = config_path.Substring(start_i + 2, file_end_i - (start_i + 2));
						}

						string absolute_config_file = Path.GetFullPath(config_file_part);
						root_path = Path.GetDirectoryName(absolute_config_file);
					} else {
						// This means the configuration file isn't sitting in the local file
						// system, so we assume root is the current directory.
						root_path = Environment.CurrentDirectory;
					}

					// Get the configuration bundle that was set as the path,
					DefaultDbConfig config = new DefaultDbConfig(root_path);
					try {
						config.LoadFromStream(config_in);
						config_in.Close();
					} catch (IOException e) {
						throw new DataException("Error reading configuration file: " +
											   config_path + " Reason: " + e.Message);
					}

					// Parse the url variables
					ParseEncodedVariables(url_vars, info);

					bool create_db = info.GetProperty("create", "").Equals("true");
					bool create_db_if_not_exist = info.GetProperty("boot_or_create", "").Equals("true") ||
												  info.GetProperty("create_or_boot", "").Equals("true");

					// Include any properties from the 'info' object
					IEnumerator prop_keys = info.Keys.GetEnumerator();
					while (prop_keys.MoveNext()) {
						String key = prop_keys.Current.ToString();
						if (!key.Equals("user") && !key.Equals("password")) {
							config.SetValue(key, (String)info[key]);
						}
					}

					// Check if the database exists
					bool database_exists = local_bootable.CheckExists(config);

					// If database doesn't exist and we've been told to create it if it
					// doesn't exist, then set the 'create_db' flag.
					if (create_db_if_not_exist && !database_exists) {
						create_db = true;
					}

					// Error conditions;
					// If we are creating but the database already exists.
					if (create_db && database_exists) {
						throw new DataException(
							"Can not create database because a database already exists.");
					}
					// If we are booting but the database doesn't exist.
					if (!create_db && !database_exists) {
						throw new DataException(
							"Can not find a database to start.  Either the database needs to " +
							"be created or the 'database_path' property of the configuration " +
							"must be set to the location of the data files.");
					}

					// Are we creating a new database?
					if (create_db) {
						String username = info.GetProperty("user", "");
						String password = info.GetProperty("password", "");

						db_interface = local_bootable.Create(username, password, config);
					}
						// Otherwise we must be logging onto a database,
					else {
						db_interface = local_bootable.Boot(config);
					}
				}

				// Make up the return parameters.
				Object[] ret = new Object[2];
				ret[0] = db_interface;
				ret[1] = schema_name;

				return ret;

			}
		}

		/// <summary>
		/// Creates a new <see cref="ILocalBootable"/> object that is used to manage 
		/// the connections to a database running locally.
		/// </summary>
		/// <remarks>
		/// This uses reflection to create a new <see cref="DefaultLocalBootable"/> object. We use 
		/// reflection here because we don't want to make a source level dependency link to the class.
		/// </remarks>
		/// <exception cref="DataException">
		/// If the class <c>DefaultLocalBootable</c> was not found.
		/// </exception>
		private static ILocalBootable CreateDefaultLocalBootable() {
			try {
				Type c = Type.GetType("Deveel.Data.Server.DefaultLocalBootable");
				return (ILocalBootable)Activator.CreateInstance(c);
			} catch (Exception) {
				// A lot of people ask us about this error so the message is verbose.
				throw new DataException(
					"I was unable to find the class that manages local database " +
					"connections.  This means you may not have included the correct " +
					"library in your references.");
			}
		}

		/// <summary>
		/// Given a URL encoded arguments string, this will extract the var=value
		/// pairs and write them in the given Properties object.
		/// </summary>
		/// <param name="url_vars"></param>
		/// <param name="info"></param>
		/// <remarks>
		/// For example, the string 'create=true&amp;user=usr&amp;password=passwd' will 
		/// extract the three values and write them in the Properties object.
		/// </remarks>
		private static void ParseEncodedVariables(String url_vars, Properties info) {
			// Parse the url variables.
			string[] tok = url_vars.Split('&');
			for (int i = 0; i < tok.Length; i++) {
				String token = tok[i].Trim();
				int split_point = token.IndexOf("=");
				if (split_point > 0) {
					String key = token.Substring(0, split_point).ToLower();
					String value = token.Substring(split_point + 1);
					// Put the key/value pair in the 'info' object.
					info[key] = value;
				} else {
					Console.Error.WriteLine("Ignoring url variable: '" + token + "'");
				}
			} // while

		}


		///<summary>
		/// Toggles strict get object.
		///</summary>
		/// <remarks>
		/// If the <see cref="DbDataReader.GetValue"/> method should return the 
		/// raw object type (eg. <see cref="BigDecimal"/> for integer, <see cref="string"/>
		/// for chars, etc) then this is set to false. If this is true (the default) the 
		/// <see cref="DbDataReader.GetValue"/> methods return the correct object types 
		/// as specified by the ADO.NET specification.
		/// </remarks>
		public bool IsStrictGetValue {
			get { return strict_get_object; }
			set { strict_get_object = value; }
		}

		///<summary>
		/// Toggles verbose column names from <see cref="DbDataReader.GetName"/>.
		///</summary>
		/// <remarks>
		/// If this is set to true, <see cref="DbDataReader.GetName"/> will return 
		/// <c>APP.Part.id</c> for a column name. If it is false <see cref="DbDataReader.GetName"/> 
		/// will return <c>id</c>. This property is for compatibility with older versions.
		/// </remarks>
		public bool VerboseColumnNames {
			get { return verbose_column_names; }
			set { verbose_column_names = value; }
		}

		///<summary>
		/// Toggles whether this connection is handling identifiers as case
		/// insensitive or not. 
		///</summary>
		/// <remarks>
		/// If this is true then <see cref="DbDataReader.GetString">GetString("app.id")</see> 
		/// will match against <c>APP.id</c>, etc.
		/// </remarks>
		public bool IsCaseInsensitiveIdentifiers {
			set { case_insensitive_identifiers = value; }
			get { return case_insensitive_identifiers; }
		}

		/// <summary>
		/// Returns the row Cache object for this connection.
		/// </summary>
		internal RowCache RowCache {
			get { return row_cache; }
		}

		///<summary>
		/// Closes this connection by calling the <see cref="IDisposable.Dispose"/> method 
		/// in the database interface.
		///</summary>
		public void InternalClose() {
			lock (@lock) {
				if (!IsClosed) {
					try {
						db_interface.Dispose();
					} finally {
						is_closed = true;
					}
				}
			}
		}


		///<summary>
		/// Attempts to login to the database interface with the given 
		/// default schema, username and password.
		///</summary>
		///<param name="default_schema"></param>
		///<param name="username"></param>
		///<param name="password"></param>
		///<exception cref="DataException">
		/// If the authentication fails.
		/// </exception>
		public void Login(String default_schema, String username, String password) {

			lock (@lock) {
				if (!is_closed) {
					throw new DataException("Unable to login to connection because it is open.");
				}
			}

			if (username == null || username.Equals("") ||
				password == null || password.Equals("")) {
				throw new DataException("username or password have not been set.");
			}

			// Set the default schema to username if it's null
			if (default_schema == null) {
				default_schema = username;
			}

			// Login with the username/password
			bool li = db_interface.Login(default_schema, username, password, this);
			lock (@lock) {
				is_closed = !li;
			}
			if (!li) {
				throw new DataException("User authentication failed for: " + username);
			}

			// Determine if this connection is case insensitive or not,
			IsCaseInsensitiveIdentifiers = false;
			IDbCommand stmt = CreateCommand();
			stmt.CommandText = "SHOW CONNECTION_INFO";
			IDataReader rs = stmt.ExecuteReader();
			while (rs.Read()) {
				String key = rs.GetString(0);
				if (key.Equals("case_insensitive_identifiers")) {
					String val = rs.GetString(1);
					IsCaseInsensitiveIdentifiers = val.Equals("true");
				} else if (key.Equals("auto_commit")) {
					String val = rs.GetString(1);
					auto_commit = val.Equals("true");
				}
			}
			rs.Close();
		}

		/// <summary>
		/// Returns the url string used to make this connection.
		/// </summary>
		internal string Url {
			get { return url; }
		}

		/// <summary>
		/// Logs into the server running on a remote machine.
		/// </summary>
		/// <param name="info"></param>
		/// <param name="default_schema"></param>
		/// <exception cref="DataException">
		/// If user authentication fails.
		/// </exception>
		internal void Login(Properties info, String default_schema) {

			String username = info.GetProperty("user", "");
			String password = info.GetProperty("password", "");

			Login(default_schema, username, password);
		}

		/// <summary>
		/// Uploads any streamable objects found in an SQLQuery into the database.
		/// </summary>
		/// <param name="sql"></param>
		private void UploadStreamableObjects(SQLQuery sql) {
			// Push any streamable objects that are present in the query onto the
			// server.
			Object[] vars = sql.Variables;
			try {
				for (int i = 0; i < vars.Length; ++i) {
					// For each streamable object.
					if (vars[i] != null && vars[i] is Data.StreamableObject) {
						// Buffer size is fixed to 64 KB
						const int BUF_SIZE = 64 * 1024;

						Data.StreamableObject s_object = (Data.StreamableObject)vars[i];
						long offset = 0;
						byte type = s_object.Type;
						long total_len = s_object.Size;
						long id = s_object.Identifier;
						byte[] buf = new byte[BUF_SIZE];

						// Get the InputStream from the StreamableObject hold
						Object sob_id = id;
						InputStream i_stream = (InputStream)s_object_hold[sob_id];
						if (i_stream == null) {
							throw new Exception(
								"Assertion failed: Streamable object InputStream is not available.");
						}

						while (offset < total_len) {
							// Fill the buffer
							int index = 0;
							int block_read = (int)System.Math.Min((long)BUF_SIZE, (total_len - offset));
							int to_read = block_read;
							while (to_read > 0) {
								int count = i_stream.Read(buf, index, to_read);
								if (count == -1) {
									throw new IOException("Premature end of stream.");
								}
								index += count;
								to_read -= count;
							}

							// Send the part of the streamable object to the database.
							db_interface.PushStreamableObjectPart(type, id, total_len,
																  buf, offset, block_read);
							// Increment the offset and upload the next part of the object.
							offset += block_read;
						}

						// Remove the streamable object once it has been written
						s_object_hold.Remove(sob_id);

						//        [ Don't close the input stream - we may only want to WriteByte a part of
						//          the stream into the database and keep the file open. ]
						//          // Close the input stream
						//          i_stream.close();

					}
				}
			} catch (IOException e) {
				Console.Error.WriteLine(e.Message);
				Console.Error.WriteLine(e.StackTrace);
				throw new DataException("IO Error pushing large object to server: " +
										e.Message);
			}
		}

		/// <summary>
		/// Sends the batch of SQLQuery objects to the database to be executed.
		/// </summary>
		/// <param name="queries"></param>
		/// <param name="results">The consumer objects for the query results.</param>
		/// <remarks>
		/// If a query succeeds then we are guarenteed to know that size of the result set.
		/// <para>
		/// This method blocks until all of the queries have been processed by the database.
		/// </para>
		/// </remarks>
		internal void ExecuteQueries(SQLQuery[] queries, ResultSet[] results) {
			// For each query
			for (int i = 0; i < queries.Length; ++i) {
				ExecuteQuery(queries[i], results[i]);
			}
		}

		/// <summary>
		/// Sends the SQL string to the database to be executed.
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="result_set">The consumer for the results from the database.</param>
		/// <remarks>
		/// We are guarenteed that if the query succeeds that we know the size of the 
		/// result set and at least first first row of the set.
		/// <para>
		/// This method will block until we have received the result header information.
		/// </para>
		/// </remarks>
		internal void ExecuteQuery(SQLQuery sql, ResultSet result_set) {
			UploadStreamableObjects(sql);
			// Execute the query,
			IQueryResponse resp = db_interface.ExecuteQuery(sql);

			// The format of the result
			ColumnDescription[] col_list = new ColumnDescription[resp.ColumnCount];
			for (int i = 0; i < col_list.Length; ++i) {
				col_list[i] = resp.GetColumnDescription(i);
			}
			// Set up the result set to the result format and update the time taken to
			// execute the query on the server.
			result_set.ConnSetup(resp.ResultId, col_list, resp.RowCount);
			result_set.SetQueryTime(resp.QueryTimeMillis);
		}

		/// <summary>
		/// Called by ResultSet to query a part of a result from the server.
		/// </summary>
		/// <param name="result_id"></param>
		/// <param name="start_row"></param>
		/// <param name="count_rows"></param>
		/// <returns>
		/// Returns a <see cref="IList"/> that represents the result from the server.
		/// </returns>
		internal ResultPart RequestResultPart(int result_id, int start_row, int count_rows) {
			return db_interface.GetResultPart(result_id, start_row, count_rows);
		}

		/// <summary>
		/// Requests a part of a streamable object from the server.
		/// </summary>
		/// <param name="result_id"></param>
		/// <param name="streamable_object_id"></param>
		/// <param name="offset"></param>
		/// <param name="len"></param>
		/// <returns></returns>
		internal StreamableObjectPart RequestStreamableObjectPart(int result_id, long streamable_object_id, long offset, int len) {
			return db_interface.GetStreamableObjectPart(result_id, streamable_object_id, offset, len);
		}

		/// <summary>
		/// Disposes of the server-side resources associated with the result 
		/// set with result_id.
		/// </summary>
		/// <param name="result_id"></param>
		/// <remarks>
		/// This should be called either before we start the download of a new result set, 
		/// or when we have finished with the resources of a result set.
		/// </remarks>
		internal void DisposeResult(int result_id) {
			// Clear the row cache.
			// It would be better if we only cleared row entries with this
			// table_id.  We currently clear the entire cache which means there will
			// be traffic created for other open result sets.
			//    Console.Out.WriteLine(result_id);
			//    row_cache.clear();
			// Only dispose if the connection is open
			if (!is_closed) {
				db_interface.DisposeResult(result_id);
			}
		}

		/// <summary>
		/// Adds a <see cref="ITriggerListener"/> that listens for all triggers events with 
		/// the name given.
		/// </summary>
		/// <param name="trigger_name"></param>
		/// <param name="listener"></param>
		/// <remarks>
		/// Triggers are created with the <c>CREATE TRIGGER</c> syntax.
		/// </remarks>
		internal void AddTriggerListener(String trigger_name, ITriggerListener listener) {
			lock (trigger_list) {
				trigger_list.Add(trigger_name);
				trigger_list.Add(listener);
			}
		}

		/// <summary>
		/// Removes the <see cref="ITriggerListener"/> for the given trigger name.
		/// </summary>
		/// <param name="trigger_name"></param>
		/// <param name="listener"></param>
		internal void RemoveTriggerListener(String trigger_name, ITriggerListener listener) {
			lock (trigger_list) {
				for (int i = trigger_list.Count - 2; i >= 0; i -= 2) {
					if (trigger_list[i].Equals(trigger_name) &&
						trigger_list[i + 1].Equals(listener)) {
						trigger_list.RemoveAt(i);
						trigger_list.RemoveAt(i);
					}
				}
			}
		}

		/// <summary>
		/// Creates a <see cref="Data.StreamableObject"/> on the client side 
		/// given a <see cref="Stream"/>, and length and a type.
		/// </summary>
		/// <param name="x"></param>
		/// <param name="length"></param>
		/// <param name="type"></param>
		/// <remarks>
		/// When this method returns, a <see cref="Data.StreamableObject"/> entry will be 
		/// added to the hold.
		/// </remarks>
		/// <returns></returns>
		internal Data.StreamableObject CreateStreamableObject(Stream x, int length, byte type) {
			long ob_id;
			lock (s_object_hold) {
				ob_id = s_object_id;
				++s_object_id;
				// Add the stream to the hold and get the unique id
				s_object_hold[ob_id] = x;
			}
			// Create and return the StreamableObject
			return new Data.StreamableObject(type, length, ob_id);
		}

		/// <summary>
		/// Removes the <see cref="Data.StreamableObject"/> from the hold on the client.
		/// </summary>
		/// <param name="s_object"></param>
		/// <remarks>
		/// This should be called when the <see cref="DbCommand"/> closes.
		/// </remarks>
		internal void RemoveStreamableObject(Data.StreamableObject s_object) {
			s_object_hold.Remove(s_object.Identifier);
		}

		// NOTE: For JDBC standalone apps, the thread that calls this will be a
		//   WorkerThread.
		//   For JDBC client/server apps, the thread that calls this will by the
		//   connection thread that listens for data from the server.
		public void OnDatabaseEvent(int event_type, String event_message) {
			if (event_type == 99) {
				if (trigger_thread == null) {
					trigger_thread = new TriggerDispatchThread(this);
					trigger_thread.Start();
				}
				trigger_thread.DispatchTrigger(event_message);
			} else {
				throw new ApplicationException("Unrecognised database event: " + event_type);
			}

			//    Console.Out.WriteLine("[com.mckoi.jdbc.DbConnection] Event received:");
			//    Console.Out.WriteLine(event_type);
			//    Console.Out.WriteLine(event_message);
		}

		/// <inheritdoc/>
		public DbTransaction BeginTransaction() {
			return new DbTransaction(this);
		}

		#region Implementation of IDisposable

		public void Dispose() {
			Close();
		}

		#endregion

		#region Implementation of IDbConnection

		IDbTransaction IDbConnection.BeginTransaction() {
			return BeginTransaction();
		}

		IDbTransaction IDbConnection.BeginTransaction(IsolationLevel il) {
			if (il != IsolationLevel.Serializable)
				throw new ArgumentException();
			return BeginTransaction();
		}

		public bool IsClosed {
			get {
				lock (@lock) {
					return is_closed;
				}
			}
		}

		/// <inheritdoc/>
		public virtual void Close() {
			if (!IsClosed) {
				InternalClose();
			}
		}

		void IDbConnection.ChangeDatabase(string databaseName) {
			//TODO: support this with SET SCHEMA ...
		}

		IDbCommand IDbConnection.CreateCommand() {
			return CreateCommand();
		}

		/// <inheritdoc/>
		public DbCommand CreateCommand() {
			return new DbCommand(this);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="commandText"></param>
		/// <returns></returns>
		public DbCommand CreateCommand(string commandText) {
			DbCommand command = new DbCommand(this);
			command.CommandText = commandText;
			return command;
		}

		/// <summary>
		/// Toggles the <c>AUTO COMMIT</c> flag.
		/// </summary>
		public virtual bool AutoCommit {
			get { return auto_commit; }
			set {
				// The SQL to WriteByte into auto-commit mode.
				ResultSet result;
				if (value) {
					result = CreateCommand().ExecuteQuery("SET AUTO COMMIT ON");
					auto_commit = true;
					result.Close();
				}
				else {
					result = CreateCommand().ExecuteQuery("SET AUTO COMMIT OFF");
					auto_commit = false;
					result.Close();
				}
			}
		}

		void IDbConnection.Open() {
		}

		/// <inheritdoc/>
		public string ConnectionString {
			get { return url; }
			set { throw new NotImplementedException(); }
		}

		/// <inheritdoc/>
		public int ConnectionTimeout {
			get { throw new NotImplementedException(); }
		}

		string IDbConnection.Database {
			get { return null; }
		}

		public ConnectionState State {
			get { throw new NotImplementedException(); }
		}

		#endregion

		/// <summary>
		/// The thread that handles all dispatching of trigger events.
		/// </summary>
		private class TriggerDispatchThread {
			private readonly DbConnection conn;
			private readonly ArrayList trigger_messages_queue = new ArrayList();
			private readonly Thread thread;

			internal TriggerDispatchThread(DbConnection conn) {
				this.conn = conn;
				thread = new Thread(new ThreadStart(run));
				thread.IsBackground = true;
				thread.Name = "Trigger Dispatcher";
			}

			/// <summary>
			/// Dispatches a trigger message to the listeners.
			/// </summary>
			/// <param name="event_message"></param>
			internal void DispatchTrigger(String event_message) {
				lock (trigger_messages_queue) {
					trigger_messages_queue.Add(event_message);
					Monitor.PulseAll(trigger_messages_queue);
				}
			}

			// Thread run method
			private void run() {
				while (true) {
					try {
						String message;
						lock (trigger_messages_queue) {
							while (trigger_messages_queue.Count == 0) {
								try {
 									Monitor.Wait(trigger_messages_queue);
								} catch (ThreadInterruptedException) {
									/* ignore */
								}
							}
							message = (String)trigger_messages_queue[0];
							trigger_messages_queue.RemoveAt(0);
						}

						// 'message' is a message to process...
						// The format of a trigger message is:
						// "[trigger_name] [trigger_source] [trigger_fire_count]"
						//          Console.Out.WriteLine("TRIGGER EVENT: " + message);

						string[] tok = message.Split(' ');
						String trigger_name = tok[0];
						String trigger_source = tok[1];
						String trigger_fire_count = tok[2];

						ArrayList fired_triggers = new ArrayList();
						// Create a list of Listener's that are listening for this trigger.
						lock (conn.trigger_list) {
							for (int i = 0; i < conn.trigger_list.Count; i += 2) {
								String to_listen_for = (String)conn.trigger_list[i];
								if (to_listen_for.Equals(trigger_name)) {
									ITriggerListener listener =
										(ITriggerListener)conn.trigger_list[i + 1];
									// NOTE, we can't call 'listener.OnTriggerFired' here because
									// it's not a good idea to call user code when we are
									// synchronized over 'trigger_list' (deadlock concerns).
									fired_triggers.Add(listener);
								}
							}
						}

						// Fire them triggers.
						for (int i = 0; i < fired_triggers.Count; ++i) {
							ITriggerListener listener =
								(ITriggerListener)fired_triggers[i];
							listener.OnTriggerFired(trigger_name);
						}

					} catch (Exception t) {
						Console.Error.WriteLine(t.Message); 
						Console.Error.WriteLine(t.StackTrace);
					}

				}
			}

			public void Start() {
				thread.Start();
			}
		}

		internal const int DRIVER_MAJOR_VERSION = 1;
		internal const int DRIVER_MINOR_VERSION = 0;

		/// <summary>
		/// The timeout for a query in seconds.
		/// </summary>
		internal static int QUERY_TIMEOUT = Int32.MaxValue;
	}
}