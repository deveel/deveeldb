//  
//  ConnectionString.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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
using System.Text;

namespace Deveel.Data.Client {
	/// <summary>
	/// A class that encapsulates all the properties needed to build a valid
	/// string to open a <see cref="DeveelDbConnection">connection</see> to
	/// a local or remote database.
	/// </summary>
	public sealed class ConnectionString {
		/// <summary>
		/// Constructs an empty <see cref="ConnectionString"/>.
		/// </summary>
		public ConnectionString() {
			properties = new Hashtable();
			addProps = new Hashtable();
		}

		/// <summary>
		/// Constructs a <see cref="ConnectionString"/> for the given credentials
		/// and to the host identified.
		/// </summary>
		/// <param name="host">The address of the server to connect to.</param>
		/// <param name="port">The number of the port on which the database server
		/// listens for connections.</param>
		/// <param name="username">The name used to identify the user within the database 
		/// context given.</param>
		/// <param name="password"></param>
		public ConnectionString(string host, int port, string username, string password)
			: this() {
			Host = host;
			Port = port;
			UserName = username;
			Password = password;
		}

		/// <summary>
		/// Constructs a <see cref="ConnectionString"/> to the host given.
		/// </summary>
		/// <param name="host">The address of the server to connect to.</param>
		/// <param name="port">The number of the port on which the database server
		/// listens for connections.</param>
		public ConnectionString(string host, int port)
			: this(host, port, null, null) {
		}

		/// <summary>
		/// Constructs a <see cref="ConnectionString"/> using the string representation
		/// provided as base.
		/// </summary>
		/// <param name="s">The formatted string to be parsed.</param>
		public ConnectionString(string s)
			: this() {
			if (s == null)
				throw new ArgumentNullException("s");

			Parse(s);
		}

		private ConnectionString(bool readOnly)
			: this() {
			this.readOnly = readOnly;
		}

		static ConnectionString() {
			DefaultKeys = new ArrayList();
			DefaultKeys.Add(HostKey.ToLower());
			DefaultKeys.Add(PortKey.ToLower());
			DefaultKeys.Add(UserNameKey.ToLower());
			DefaultKeys.Add(PasswordKey.ToLower());
			DefaultKeys.Add(SchemaKey.ToLower());
			DefaultKeys.Add(CreateKey.ToLower());
			DefaultKeys.Add(BootOrCreateKey.ToLower());
			DefaultKeys.Add(ParameterStyleKey.ToLower());
			DefaultKeys.Add(PathKey.ToLower());
		}

		private readonly Hashtable properties;
		private string connection_string;
		private bool addPropsDirty;
		private readonly Hashtable addProps;
		private readonly bool readOnly;

		private const string HostKey = "Host";
		private const string PortKey = "Port";
		private const string DatabaseKey = "Database";
		private const string UserNameKey = "UserName";
		private const string PasswordKey = "Password";
		private const string SchemaKey = "Schema";
		private const string CreateKey = "Create";
		private const string BootOrCreateKey = "BootOrCreate";
		private const string ParameterStyleKey = "ParameterStyle";
		private const string PathKey = "Path";

		private static readonly ArrayList DefaultKeys;

		/// <summary>
		/// The default port number for a network connection (9157), used when
		/// not explicitely specified.
		/// </summary>
		/// <seealso cref="Port"/>
		public const int DefaultPort = 9157;

		/// <summary>
		/// The string identifying a connection to a local database.
		/// </summary>
		/// <seealso cref="Host"/>
		public const string LocalHost = "Local";

		/// <summary>
		/// The name of the default schema to use when it is not specified
		/// by a user.
		/// </summary>
		/// <seealso cref="Schema"/>
		public const string DefaultSchema = "APP";

		/// <summary>
		/// An empty and immutable connection string.
		/// </summary>
		public static readonly ConnectionString Empty = new ConnectionString(true);

		/// <summary>
		/// Gets an immutable key/value collection that describes the
		/// connection properties.
		/// </summary>
		public IDictionary Properties {
			get { return(IDictionary) properties.Clone(); }
		}

		/// <summary>
		/// Gets a key/value pairs collection used to store non-standard
		/// connection properties.
		/// </summary>
		public IDictionary AdditionalProperties {
			get {
				if (addPropsDirty) {
					addProps.Clear();

					foreach (DictionaryEntry entry in properties) {
						string testKey = entry.Key.ToString().ToLower();
						if (!DefaultKeys.Contains(testKey))
							addProps[entry.Key] = entry.Value;
					}
				}

				return addProps;
			}
		}

		/// <summary>
		/// Gets or sets the name used to identify a user within a database context.
		/// </summary>
		public string UserName {
			get { return (string)properties[UserNameKey]; }
			set {
				CheckReadOnly();
				if (value == null)
					throw new ArgumentNullException("value");
				properties[UserNameKey] = value;
			}
		}

		/// <summary>
		/// Gets or sets the password used to identify the user within the database context.
		/// </summary>
		/// <seealso cref="UserName"/>
		public string Password {
			get { return (string)properties[PasswordKey]; }
			set {
				CheckReadOnly();
				if (value == null && properties.ContainsKey(PasswordKey)) {
					properties.Remove(PasswordKey);
					return;
				}
				if (value == null)
					throw new ArgumentNullException("value");

				properties[PasswordKey] = value;
			}
		}

		/// <summary>
		/// Gets or sets the host address of the connection.
		/// </summary>
		/// <remarks>
		/// This has to be set to <c>Local</c> if the connection must be
		/// issued to a local database.
		/// </remarks>
		public string Host {
			get { return (string)properties[HostKey]; }
			set {
				CheckReadOnly();
				if (value == null) {
					properties.Remove(HostKey);
				} else {
					properties[HostKey] = value;
				}
			}
		}

		/// <summary>
		/// Gets or sets the default schema to which connect.
		/// </summary>
		public string Schema {
			get { return (string) properties[SchemaKey]; }
			set {
				CheckReadOnly();
				if (value == null) {
					properties.Remove(SchemaKey);
				} else {
					properties[SchemaKey] = value;
				}
			}
		}

		/// <summary>
		/// Gets or sets whether the connection is to a local database.
		/// </summary>
		/// <seealso cref="Host"/>
		/// <seealso cref="Path"/>
		public bool IsLocal {
			get {
				string host = Host;
				return host != null && host.Length != 0 && String.Compare(LocalHost, host, true) == 0;
			}
			set {
				CheckReadOnly();
				if (value) {
					properties[HostKey] = LocalHost;
				} else if (String.Compare(Host, LocalHost, true) == 0) {
					properties.Remove(HostKey);
				}
			}
		}

		/// <summary>
		/// When a connection is issued to a local database, this property
		/// is used to control the path to the configuration file of the database.
		/// </summary>
		public string Path {
			get {
				object value = properties[PathKey];
				return (value == null ? null : (string) value);
			}
			set {
				CheckReadOnly();
				properties[PathKey] = value;
			}
		}

		/// <summary>
		/// Gets or sets the port used for a network connection.
		/// </summary>
		public int Port {
			get {
				object value = properties[PortKey];
				return (value == null ? DefaultPort : (int)value);
			}
			set {
				CheckReadOnly();
				if (value < 0)
					value = DefaultPort;
				properties[PortKey] = value;
			}
		}

		/// <summary>
		/// Dictates the local or remote system whether the connection must create
		/// a new database when opened.
		/// </summary>
		/// <remarks>
		/// This only works if the connection <see cref="IsLocal">is local</see>.
		/// </remarks>
		public bool Create {
			get {
				object value = properties[CreateKey];
				return (value == null ? true : (bool) value);
			}
			set {
				CheckReadOnly();
				properties[CreateKey] = value;
			}
		}

		/// <summary>
		/// Dictates the local or remote system whether the connection must create
		/// a new database when opened, if does not exist already, or otherwise boot it.
		/// </summary>
		/// <remarks>
		/// This only works if the connection <see cref="IsLocal">is local</see>.
		/// </remarks>
		/// <seealso cref="Create"/>
		public bool BootOrCreate {
			get {
				object value = properties[BootOrCreateKey];
				return (value == null ? true : (bool) value);
			}
			set {
				CheckReadOnly();
				properties[BootOrCreateKey] = value;
			}
		}

		/// <summary>
		/// Gets or sets the style of parameters in prepared statements.
		/// </summary>
		/// <seealso cref="ParameterStyle"/>
		public ParameterStyle ParameterStyle {
			get {
				object value = properties[ParameterStyleKey];
				return (value == null ? ParameterStyle.Marker : (ParameterStyle) value);
			}
			set {
				CheckReadOnly();
				properties[ParameterStyleKey] = value;
			}
		}

		/// <summary>
		/// Gets or sets the name of the database to connect to.
		/// </summary>
		public string Database {
			get {
				object value = properties[DatabaseKey];
				return (value == null ? String.Empty : (string) value);
			}
			set {
				CheckReadOnly();
				properties[DatabaseKey] = value;
			}
		}

		private void CheckReadOnly() {
			if (readOnly)
				throw new InvalidOperationException("The connection string is readonly.");
		}

		/// <summary>
		/// Sets a given connection property.
		/// </summary>
		/// <param name="key">The key of the </param>
		/// <param name="value"></param>
		public void SetProperty(string key, object value) {
			if (key == null)
				throw new ArgumentNullException("key");

			CheckReadOnly();
			IntSetProperty(key, value);
		}

		/// <summary>
		/// Returns a string representation of the connection string.
		/// </summary>
		/// <returns>
		/// Returns a valid formatted <see cref="string"/> for connecting
		/// to local or remote database.
		/// </returns>
		public override string ToString() {
			if (connection_string == null) {
				if (properties.Count == 0)
					return String.Empty;

				StringBuilder sb = new StringBuilder();
				int i = 0;
				foreach (DictionaryEntry entry in properties) {
					sb.Append(entry.Key);
					sb.Append('=');
					sb.Append(entry.Value);

					if (++i < properties.Count - 1)
						sb.Append(';');
				}

				connection_string = sb.ToString();
			}

			return connection_string;
		}

		private void Parse(string s) {
			if (s.Length == 0)
				throw new FormatException("The connection string is not well formatted.");

			Hashtable keyValues = ParseKeyValuePairs(s);
			foreach (DictionaryEntry entry in keyValues) {
				IntSetProperty((string)entry.Key, entry.Value);
			}
		}

		private void IntSetProperty(string key, object value) {
			string testKey = key.ToLower();
			switch (testKey) {
				case "host":
				case "server":
				case "address":
					Host = Convert.ToString(value);
					break;
				case "port":
					Port = Convert.ToInt32(value);
					break;
				case "default schema":
				case "schema":
					Schema = Convert.ToString(value);
					break;
				case "create":
					Create = Convert.ToBoolean(value);
					break;
				case "bootorcreate":
				case "createorboot":
					BootOrCreate = Convert.ToBoolean(value);
					break;
				case "user":
				case "username":
				case "userid":
				case "uid":
					UserName = Convert.ToString(value);
					break;
				case "password":
				case "secret":
				case "pass":
					Password = Convert.ToString(value);
					break;
				case "parameterstyle":
				case "useparameter":
				case "paramstyle":
					ParameterStyle = (ParameterStyle) Enum.Parse(typeof (ParameterStyle), value.ToString(), true);
					break;
				case "path":
				case "confpath":
				case "configuration path":
				case "config path":
					Path = Convert.ToString(value);
					break;
				default:
					properties[key] = value;
					addPropsDirty = true;
					break;
			}
		}

		private static Hashtable ParseKeyValuePairs(string src) {
			String[] keyvalues = src.Split(';');
			String[] newkeyvalues = new String[keyvalues.Length];
			int x = 0;

			// first run through the array and check for any keys that
			// have ; in their value
			foreach (String keyvalue in keyvalues) {
				// check for trailing ; at the end of the connection string
				if (keyvalue.Length == 0) continue;

				// this value has an '=' sign so we are ok
				if (keyvalue.IndexOf('=') >= 0) {
					newkeyvalues[x++] = keyvalue;
				} else {
					newkeyvalues[x - 1] += ";";
					newkeyvalues[x - 1] += keyvalue;
				}
			}

			Hashtable hash = new Hashtable();

			// now we run through our normalized key-values, splitting on equals
			for (int y = 0; y < x; y++) {
				String[] parts = newkeyvalues[y].Split('=');

				// first trim off any space and lowercase the key
				parts[0] = parts[0].Trim().ToLower();
				parts[1] = parts[1].Trim();

				// we also want to clear off any quotes
				parts[0] = parts[0].Trim('\'', '"');
				parts[1] = parts[1].Trim('\'', '"');

				hash.Add(parts[0], parts[1]);
			}
			return hash;
		}
	}
}