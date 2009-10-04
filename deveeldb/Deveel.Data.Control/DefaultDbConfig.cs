//  
//  DefaultDbConfig.cs
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
using System.IO;
using System.Net;

using Deveel.Data.Util;

namespace Deveel.Data.Control {
	///<summary>
	/// Implements a default database configuration that is useful for 
	/// setting up a database.
	///</summary>
	/// <remarks>
	/// This configuration object is mutable.
	/// </remarks>
	public class DefaultDbConfig : DbConfig {
		///<summary>
		/// Constructs the configuration.
		///</summary>
		///<param name="current_path">The current path of the configuration in the file 
		/// system. This is useful if the configuration is based on a file with relative 
		/// paths set in it.</param>
		public DefaultDbConfig(string current_path)
			: base(current_path) {
		}

		///<summary>
		/// Constructs the configuration with the current system path as the configuration path.
		///</summary>
		public DefaultDbConfig()
			: this(".") {
		}

		/// <inheritdoc/>
		protected override String GetDefaultValue(String property_key) {
			ConfigProperty property = (ConfigProperty)ConfigDefaults[property_key];
			return property == null ? null : property.DefaultValue;
		}

		///<summary>
		/// Loads all the configuration values from the given <see cref="Stream"/>.
		///</summary>
		///<param name="input"></param>
		/// <remarks>
		/// The input stream must be formatted in a standard properties format.
		/// </remarks>
		public void LoadFromStream(Stream input) {
			if (!input.CanRead)
				throw new ArgumentException();

			Properties config = new Properties();
			config.Load(new BufferedStream(input));
			// For each property in the file
			IEnumerator en = config.PropertyNames.GetEnumerator();
			while (en.MoveNext()) {
				// Set the property value in this configuration.
				String property_key = (String)en.Current;
				SetValue(property_key, config.GetProperty(property_key));
			}
		}

		///<summary>
		/// Loads all the configuration settings from a configuration file.
		///</summary>
		///<param name="configuration_file"></param>
		/// <remarks>
		/// Useful if you want to load a default configuration from a <i>db.conf</i> file. 
		/// The file must be formatted in a standard properties format.
		/// </remarks>
		public void LoadFromFile(string configuration_file) {
			FileStream file_in = new FileStream(configuration_file, FileMode.Open, FileAccess.Read, FileShare.Read);
			LoadFromStream(file_in);
			file_in.Close();
		}

		///<summary>
		/// Loads all the configuration values from the given URL.
		///</summary>
		///<param name="configuration_url"></param>
		/// <remarks>
		/// The file must be formatted in a standard properties format.
		/// </remarks>
		public void loadFromURL(Uri configuration_url) {
			WebRequest request = WebRequest.Create(configuration_url);
			WebResponse response = request.GetResponse();
			LoadFromStream(response.GetResponseStream());
			response.Close();
		}

		// ---------- Variable helper setters ----------

		/// <summary>
		/// Gets or sets the path to the database.
		/// </summary>
		public string DatabasePath {
			set { SetValue("database_path", value); }
			get { return GetValue("database_path"); }
		}

		///<summary>
		/// Gets or sets the path of the log.
		///</summary>
		public string LogPath {
			get { return GetValue("log_path"); }
			set { SetValue("log_path", value); }
		}

		/// <summary>
		/// Gets or sets that the engine ignores case for identifiers.
		/// </summary>
		public bool IgnoreIdentifierCase {
			get {
				string value = GetValue("ignore_case_for_identifiers");
				return (value == "enabled" ? true : false);
			}
			set { SetValue("ignore_case_for_identifiers", value ? "enabled" : "disabled"); }
		}

		/// <summary>
		/// Gets or sets that the database is read-only.
		/// </summary>
		public bool ReadOnly {
			get {
				string value = GetValue("read_only");
				return (value == "enabled" ? true : false);
			}
			set { SetValue("read_only", value ? "enabled" : "disabled"); }
		}

		/// <summary>
		/// Gets or sets the minimum debug level for output to the debug log file.
		/// </summary>
		public int MinimumDebugLevel {
			get {
				string value = GetValue("debug_level");
				return (value == null ? -1 : Int32.Parse(value));
			}
			set { SetValue("debug_level", value.ToString()); }
		}

		// ---------- Statics ----------

		/// <summary>
		/// A <see cref="Hashtable"/> of default configuration values.
		/// </summary>
		/// <remarks>
		/// This maps from property_key to <see cref="ConfigProperty"/> object that 
		/// describes the property.
		/// </remarks>
		private static readonly Hashtable ConfigDefaults = new Hashtable();

		/// <summary>
		/// Adds a default property to the <see cref="ConfigDefaults"/> map.
		/// </summary>
		/// <param name="property"></param>
		private static void AddDefProperty(ConfigProperty property) {
			ConfigDefaults.Add(property.Key, property);
		}

		/// <summary>
		/// Sets up the <see cref="ConfigDefaults"/> map with default configuration values.
		/// </summary>
		static DefaultDbConfig() {
			AddDefProperty(new ConfigProperty("database_path", "./data", "PATH"));
			// AddDefProperty(new ConfigProperty("log_path", "./log", "PATH"));
			AddDefProperty(new ConfigProperty("root_path", "jvm", "STRING"));
			AddDefProperty(new ConfigProperty("server_port", "9157", "STRING"));
			AddDefProperty(new ConfigProperty("ignore_case_for_identifiers", "disabled", "BOOLEAN"));
			AddDefProperty(new ConfigProperty("regex_library", "gnu.regexp", "STRING"));
			AddDefProperty(new ConfigProperty("data_cache_size", "4194304", "INT"));
			AddDefProperty(new ConfigProperty("max_cache_entry_size", "8192", "INT"));
			AddDefProperty(new ConfigProperty("lookup_comparison_list", "enabled", "BOOLEAN"));
			AddDefProperty(new ConfigProperty("maximum_worker_threads", "4", "INT"));
			AddDefProperty(new ConfigProperty("dont_synch_filesystem", "disabled", "BOOLEAN"));
			AddDefProperty(new ConfigProperty("transaction_error_on_dirty_select", "enabled", "BOOLEAN"));
			AddDefProperty(new ConfigProperty("read_only", "disabled", "BOOLEAN"));
			AddDefProperty(new ConfigProperty("debug_log_file", "debug.log", "FILE"));
			AddDefProperty(new ConfigProperty("debug_level", "20", "INT"));
			AddDefProperty(new ConfigProperty("table_lock_check", "enabled", "BOOLEAN"));
		}

		// ---------- Inner classes ----------

		/// <summary>
		/// An object the describes a single configuration property and the 
		/// default value for it.
		/// </summary>
		private sealed class ConfigProperty {

			private readonly string key;
			private readonly string default_value;
			private readonly string type;
			private readonly string comment;

			internal ConfigProperty(string key, string default_value, string type, string comment) {
				this.key = key;
				this.default_value = default_value;
				this.type = type;
				this.comment = comment;
			}

			internal ConfigProperty(string key, string default_value, string type)
				: this(key, default_value, type, null) {
			}

			public string Key {
				get { return key; }
			}

			public string DefaultValue {
				get { return default_value; }
			}

			public string Type {
				get { return type; }
			}

			public string Comment {
				get { return comment; }
			}
		}
	}
}