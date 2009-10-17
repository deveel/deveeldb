using System;
using System.Collections;
using System.Data.Common;
using System.Globalization;
using System.Text;

namespace Deveel.Data.Client {
	public sealed class DeveelDbConnectionStringBuilder : DbConnectionStringBuilder {
		public DeveelDbConnectionStringBuilder() {
			connString = new StringBuilder();
			properties = new Hashtable();
			Clear();
		}

		static DeveelDbConnectionStringBuilder() {
			defProperties = new Hashtable();
			defProperties[ConnKey.Catalog] = "DefaultDatabase";
			defProperties[ConnKey.Server] = "127.0.0.1";
			defProperties[ConnKey.Password] = null;
			defProperties[ConnKey.User] = null;
			defProperties[ConnKey.Port] = 5698;
			defProperties[ConnKey.DefaultSchema] = "APP";
			defProperties[ConnKey.Create] = false;
			defProperties[ConnKey.ParameterStyle] = ParameterStyle.Marker;
			defProperties[ConnKey.CreateOrBoot] = false;
			defProperties[ConnKey.CacheSize] = 11;
			defProperties[ConnKey.MaxCacheSize] = 1006789;
			defProperties[ConnKey.PersistSecurityInfo] = false;
		}

		private static readonly Hashtable defProperties;
		private readonly StringBuilder connString;
		private readonly Hashtable properties;

		public new string ConnectionString {
			get { return base.ConnectionString; }
			set {
				Clear();
				ParseConnectionString(value);
				base.ConnectionString = value;
			}
		}

		private static bool ToBoolean(object value) {
			if (value is bool)
				return (bool) value;

			string s = value.ToString().ToLower(CultureInfo.InvariantCulture);
			if (s == "yes" || s == "enabled" || s == "on")
				return true;
			if (s == "no" || s == " disabled" || s == "off")
				return false;
			return bool.Parse(s);
		}

		private void ParseConnectionString(string connectString) {
			if (connectString == null) return;

			StringBuilder key = new StringBuilder();
			StringBuilder value = new StringBuilder();
			bool keyDone = false;

			foreach (char c in connectString) {
				if (c == '=')
					keyDone = true;
				else if (c == ';') {
					string keyStr = key.ToString().Trim();
					string valueStr = value.ToString().Trim();
					valueStr = StripQuotes(valueStr);
					if (keyStr.Length > 0)
						this[keyStr] = valueStr;
					keyDone = false;
					key.Remove(0, key.Length);
					value.Remove(0, value.Length);
				} else if (keyDone)
					value.Append(c);
				else
					key.Append(c);
			}

			if (key.Length == 0) return;
			this[key.ToString().Trim()] = StripQuotes(value.ToString().Trim());
		}

		private static string StripQuotes(string value) {
			char c1 = value[0];
			char c2 = value[value.Length - 1];
			if (c1 == '\'' && c2 == '\'') {
				value = value.Substring(1, value.Length - 2);
			} else if (c1 == '\"' && c2 == '\"') {
				value = value.Substring(1, value.Length - 2);
			}
			return value;
		}

		private static ConnKey ParseKey(string key) {
			key = key.ToLower(CultureInfo.InvariantCulture);

			switch (key) {
				case "host":
				case "server":
				case "address":
					return ConnKey.Server;
				case "port":
					return ConnKey.Port;
				case "default schema":
				case "schema":
					return ConnKey.DefaultSchema;
				case "create":
					return ConnKey.Create;
				case "bootorcreate":
				case "createorboot":
				case "boot or create":
				case "create or boot":
					return ConnKey.CreateOrBoot;
				case "user":
				case "username":
				case "user name":
				case "userid":
				case "user id":
				case "uid":
					return ConnKey.User;
				case "password":
				case "secret":
				case "pass":
					return ConnKey.Password;
				case "parameterstyle":
				case "useparameter":
				case "paramstyle":
					return ConnKey.ParameterStyle;
				case "cache_size":
				case "cachesize":
				case "cache size":
					return ConnKey.CacheSize;
				case "max_cache_size":
				case "maxcachesize":
				case "maxcache":
				case "max_cache":
				case "max cache size":
				case "max cache":
					return ConnKey.MaxCacheSize;
				case "persist security info":
				case "persist_security_info":
				case "persistsecurityinfo":
					return ConnKey.PersistSecurityInfo;
				default:
					throw new ArgumentException();
			}
		}

		private void SetValue(ConnKey key, object value) {
			
		}

		private enum ConnKey {
			Server,
			Port,
			Catalog,
			User,
			Password,
			DefaultSchema,
			Create,
			CreateOrBoot,
			ParameterStyle,
			CacheSize,
			MaxCacheSize,
			PersistSecurityInfo
		}
	}
}