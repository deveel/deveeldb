using System;
using System.Collections;
using System.Text;

namespace Deveel.Data.Client {
	public sealed class ConnectionString {
		public ConnectionString() {
			properties = new Hashtable();
			addProps = new Hashtable();
		}

		public ConnectionString(string host, int port, string username, string password)
			: this() {
			Host = host;
			Port = port;
			UserName = username;
			Password = password;
		}

		public ConnectionString(string host, int port)
			: this(host, port, null, null) {
		}

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
		}

		private readonly Hashtable properties;
		private string connection_string;
		private bool addPropsDirty;
		private readonly Hashtable addProps;
		private readonly bool readOnly;

		private const string HostKey = "Host";
		private const string PortKey = "Port";
		private const string UserNameKey = "UserName";
		private const string PasswordKey = "Password";
		private const string SchemaKey = "Schema";
		private const string CreateKey = "Create";
		private const string BootOrCreateKey = "BootOrCreate";

		private static readonly ArrayList DefaultKeys;

		public const int DefaultPort = 9157;
		public const string LocalHost = "Local";
		public const string DefaultSchema = "APP";

		public static readonly ConnectionString Empty = new ConnectionString(true);

		public IDictionary Properties {
			get { return(IDictionary) properties.Clone(); }
		}

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

		public string UserName {
			get { return (string)properties[UserNameKey]; }
			set {
				CheckReadOnly();
				if (value == null)
					throw new ArgumentNullException("value");
				properties[UserNameKey] = value;
			}
		}

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

		private void CheckReadOnly() {
			if (readOnly)
				throw new InvalidOperationException("The connection string is readonly.");
		}

		public void SetProperty(string key, object value) {
			if (key == null)
				throw new ArgumentNullException("key");

			CheckReadOnly();
			IntSetProperty(key, value);
		}

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