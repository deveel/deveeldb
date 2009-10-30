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

namespace Deveel.Data.Control {
	///<summary>
	/// Implements a default database configuration that is useful for 
	/// setting up a database.
	///</summary>
	/// <remarks>
	/// This configuration object is mutable.
	/// </remarks>
	public sealed class DefaultDbConfig : DbConfig {
		///<summary>
		/// Constructs the configuration.
		///</summary>
		///<param name="current_path">The current path of the configuration in the file 
		/// system. This is useful if the configuration is based on a file with relative 
		/// paths set in it.</param>
		public DefaultDbConfig(string current_path)
			: base(current_path) {
			SetValue("database_path", "./data", "PATH");
			SetValue("log_path", "./log", "PATH");
			// SetValue("name", "DefaultDatabase", "STRING");
			// SetValue("root_path", "env", "STRING");
			SetValue("server_port", "9157", "STRING");
			SetValue("server_address", "127.0.0.1", "STRING");
			SetValue("ignore_case_for_identifiers", "disabled", "BOOLEAN");
			SetValue("regex_library", "Deveel.Text.DeveelRegexLibrary", "STRING");
			SetValue("data_cache_size", "4194304", "INT");
			SetValue("max_cache_entry_size", "8192", "INT");
			SetValue("lookup_comparison_list", "enabled", "BOOLEAN");
			SetValue("maximum_worker_threads", "4", "INT");
			SetValue("dont_synch_filesystem", "disabled", "BOOLEAN");
			SetValue("transaction_error_on_dirty_select", "enabled", "BOOLEAN");
			SetValue("read_only", "disabled", "BOOLEAN");
			SetValue("debug_log_file", "debug.log", "FILE");
			SetValue("debug_level", "20", "INT");
			SetValue("table_lock_check", "enabled", "BOOLEAN");
		}

		///<summary>
		/// Constructs the configuration with the current system path as the configuration path.
		///</summary>
		public DefaultDbConfig()
			: this(".") {
		}

		// ---------- Variable helper setters ----------

		/// <summary>
		/// Gets or sets the path to the database.
		/// </summary>
		public string DatabasePath {
			set { SetValue("database_path", value); }
			get { return GetValue("database_path"); }
		}

		public string DatabaseName {
			get { return GetValue("name"); }
			set { SetValue("name", value); }
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
	}
}