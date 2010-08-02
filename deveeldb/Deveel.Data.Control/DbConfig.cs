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
using System.Collections;
using System.IO;
using System.Net;
using System.Text;

using Deveel.Data.Control;

namespace Deveel.Data {
	/// <summary>
	///  An basic implementation of <see cref="IDbConfig"/>.
	/// </summary>
	public class DbConfig : IDbConfig {
		/// <summary>
		/// The Hashtable mapping from configuration key to value for the key.
		/// </summary>
		private Hashtable key_map;

		private static DbConfig default_config;

		/// <summary>
		/// Constructs the <see cref="IDbConfig"/>.
		/// </summary>
		public DbConfig() {
			key_map = new Hashtable();
		}

		/// <summary>
		/// Gets or sets the path to the database.
		/// </summary>
		public string DatabasePath {
			set { SetValue(ConfigKeys.DatabasePath, value); }
			get { return GetValue(ConfigKeys.DatabasePath); }
		}

		///<summary>
		/// Gets or sets the path of the log.
		///</summary>
		public string LogPath {
			get { return GetValue(ConfigKeys.LogPath); }
			set { SetValue(ConfigKeys.LogPath, value); }
		}

		/// <summary>
		/// Gets or sets that the engine ignores case for identifiers.
		/// </summary>
		public bool IgnoreIdentifierCase {
			get {
				string value = GetValue(ConfigKeys.IgnoreIdentifiersCase);
				return (value == "enabled" ? true : false);
			}
			set { SetValue(ConfigKeys.IgnoreIdentifiersCase, value ? "enabled" : "disabled"); }
		}

		/// <summary>
		/// Gets or sets that the database is read-only.
		/// </summary>
		public bool ReadOnly {
			get {
				string value = GetValue(ConfigKeys.ReadOnly);
				return (value == "enabled" ? true : false);
			}
			set { SetValue(ConfigKeys.ReadOnly, value ? "enabled" : "disabled"); }
		}

		/// <summary>
		/// Gets or sets the minimum debug level for output to the debug log file.
		/// </summary>
		public int MinimumDebugLevel {
			get {
				string value = GetValue(ConfigKeys.DebugLevel);
				return (value == null ? -1 : Int32.Parse(value));
			}
			set { SetValue(ConfigKeys.DebugLevel, value.ToString()); }
		}

		private static void FormatForOutput(String str, StringBuilder buffer, bool key) {
			if (key) {
				buffer.Length = 0;
				buffer.EnsureCapacity(str.Length);
			} else
				buffer.EnsureCapacity(buffer.Length + str.Length);
			bool head = true;
			int size = str.Length;
			for (int i = 0; i < size; i++) {
				char c = str[i];
				switch (c) {
					case '\n':
						buffer.Append("\\n");
						break;
					case '\r':
						buffer.Append("\\r");
						break;
					case '\t':
						buffer.Append("\\t");
						break;
					case ' ':
						buffer.Append(head ? "\\ " : " ");
						break;
					case '\\':
					case '!':
					case '#':
					case '=':
					case ':':
						buffer.Append('\\').Append(c);
						break;
					default:
						if (c < ' ' || c > '~') {
							String hex = ((int)c).ToString("{0:x4}");
							buffer.Append("\\u0000".Substring(0, 6 - hex.Length));
							buffer.Append(hex);
						} else
							buffer.Append(c);
						break;
				}
				if (c != ' ')
					head = key;
			}
		}

		/// <summary>
		/// Returns the default value for the configuration property with the 
		/// given key.
		/// </summary>
		/// <param name="key"></param>
		/// <returns></returns>
		protected virtual String GetDefaultValue(String key) {
			// This abstract implementation returns null for all default keys.
			return null;
		}

		///<summary>
		/// Sets the configuration value for the key property key.
		///</summary>
		///<param name="key"></param>
		///<param name="value"></param>
		public void SetValue(string key, string value) {
			key_map[key] = new ConfigProperty(key, value, null);
		}

		// ---------- Implemented from IDbConfig ----------

		public string CurrentPath {
			get { 
				string value = GetValue(ConfigKeys.BasePath);
				return value == null ? "." : value;
			}
			set { SetValue(ConfigKeys.BasePath, value); }
		}

		/// <summary>
		/// Returns a <see cref="DbConfig">default</see> implementation
		/// of <see cref="IDbConfig"/> which contains all the default settings
		/// of the system.
		/// </summary>
		public static DbConfig Default {
			get {
				if (default_config == null)
					default_config = CreateDefault();
				return default_config;
			}
		}

		public String GetValue(String property_key) {
			// If the key is in the map, return it here
			ConfigProperty property = key_map[property_key] as ConfigProperty;
			if (property == null)
				return GetDefaultValue(property_key);
			return property.Value;
		}

		/// <inheritdoc/>
		public IDbConfig Merge(IDbConfig config) {
			foreach (ConfigProperty property in config) {
				if (key_map.ContainsKey(property.Key))
					continue;

				key_map[property.Key] = property.Clone();
			}

			return this;
		}

		/// <inheritdoc/>
		public object Clone() {
			DbConfig config = new DbConfig();
			config.key_map = (Hashtable) key_map.Clone();
			return config;
		}

		/// <summary>
		/// Gets an anumeration of all the key/value pairs set 
		/// in this object. 
		/// </summary>
		/// <returns>
		/// Returns an instance of <see cref="IEnumerator"/> which is used
		/// to enumerate a set of <see cref="ConfigProperty"/> which represent
		/// the configuration properties set.
		/// </returns>
		public IEnumerator GetEnumerator() {
			return key_map.Values.GetEnumerator();
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

			/*
			Properties config = new Properties();
			config.Load(new BufferedStream(input));
			// For each property in the file
			IEnumerator en = config.PropertyNames.GetEnumerator();
			while (en.MoveNext()) {
				// Set the property value in this configuration.
				String property_key = (String)en.Current;
				SetValue(property_key, config.GetProperty(property_key));
			}
			*/

			StreamReader reader = new StreamReader(input, Encoding.GetEncoding("ISO-8859-1"));
			string line;

			while ((line = reader.ReadLine()) != null) {
				char c = '\0';
				int pos = 0;

				// Trim the leading white spaces first
				while (pos < line.Length &&
					Char.IsWhiteSpace(c = line[pos]))
					pos++;

				// If empty line or begins with a comment character, skip this line.
				if ((line.Length - pos) == 0 || 
					line[pos] == '#' || 
					line[pos] == '!')
					continue;

				// The characters up to the next Whitespace, ':', or '='
				// describe the key.  But look for escape sequences.
				// Try to short-circuit when there is no escape char.
				int start = pos;
				bool needsEscape = line.IndexOf('\\', pos) != -1;
				StringBuilder key = needsEscape ? new StringBuilder() : null;
				while (pos < line.Length &&
					!Char.IsWhiteSpace(c = line[pos++]) &&
					c != '=' && c != ':') {
					if (needsEscape && c == '\\') {
						if (pos == line.Length) {
							// The line continues on the next line.  If there
							// is no next line, just treat it as a key with an
							// empty value.
							line = reader.ReadLine();
							if (line == null)
								line = "";
							pos = 0;
							while (pos < line.Length && 
								Char.IsWhiteSpace(c = line[pos]))
								pos++;
						} else {
							c = line[pos++];
							switch(c) {
								case 'n':
									key.Append('\n');
									break;
								case 't':
									key.Append('\t');
									break;
								case 'r':
									key.Append('\r');
									break;
								case 'u':
									if (pos + 4 <= line.Length) {
										char uni = (char) Convert.ToInt32(line.Substring(pos, 4), 16);
										key.Append(uni);
										pos += 4;
									} // else throw exception?
									break;
								default:
									key.Append(c);
									break;
							}
						}
					} else if (needsEscape)
						key.Append(c);
				}

				bool isDelim = (c == ':' || c == '=');

				string keyString;
				if (needsEscape)
					keyString = key.ToString();
				else if (isDelim || Char.IsWhiteSpace(c))
					keyString = line.Substring(start, (pos - 1) - start);
				else
					keyString = line.Substring(start, pos - start);

				while (pos < line.Length && 
					Char.IsWhiteSpace(c = line[pos]))
					pos++;

				if (!isDelim && (c == ':' || c == '=')) {
					pos++;
					while (pos < line.Length && 
						Char.IsWhiteSpace(c = line[pos]))
						pos++;
				}

				// Short-circuit if no escape chars found.
				if (!needsEscape) {
					SetValue(keyString, line.Substring(pos));
					continue;
				}

				// Escape char found so iterate through the rest of the line.
				StringBuilder element = new StringBuilder(line.Length - pos);
				while (pos < line.Length) {
					c = line[pos++];
					if (c == '\\') {
						if (pos == line.Length) {
							// The line continues on the next line.
							line = reader.ReadLine();

							// We might have seen a backslash at the end of
							// the file.  The JDK ignores the backslash in
							// this case, so we follow for compatibility.
							if (line == null)
								break;

							pos = 0;
							while (pos < line.Length
								   && Char.IsWhiteSpace(c = line[pos]))
								pos++;
							element.EnsureCapacity(line.Length - pos + element.Length);
						} else {
							c = line[pos++];
							switch (c) {
								case 'n':
									element.Append('\n');
									break;
								case 't':
									element.Append('\t');
									break;
								case 'r':
									element.Append('\r');
									break;
								case 'u':
									if (pos + 4 <= line.Length) {
										char uni = (char)Convert.ToInt32(line.Substring(pos, 4), 16);
										element.Append(uni);
										pos += 4;
									}        // else throw exception?
									break;
								default:
									element.Append(c);
									break;
							}
						}
					} else
						element.Append(c);
				}
				
				SetValue(keyString, element.ToString());
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
			FileStream file_in = null;
			try {
				file_in = new FileStream(configuration_file, FileMode.Open, FileAccess.Read, FileShare.Read);
				LoadFromStream(file_in);
			} finally {
				if (file_in != null)
					file_in.Close();
			}
		}

		///<summary>
		/// Loads all the configuration values from the given URL.
		///</summary>
		///<param name="configuration_url"></param>
		/// <remarks>
		/// The file must be formatted in a standard properties format.
		/// </remarks>
		public void LoadFromUrl(Uri configuration_url) {
			WebRequest request = WebRequest.Create(configuration_url);
			WebResponse response = request.GetResponse();
			LoadFromStream(response.GetResponseStream());
			response.Close();
		}

		public void SaveTo(string fileName) {
			/*
			FileStream fileStream = null;

			try {
				fileStream = new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
				fileStream.SetLength(0);
				fileStream.Seek(0, SeekOrigin.Begin);

				Properties properties = new Properties();
				foreach (ConfigProperty entry in key_map.Values) {
					properties.SetProperty(entry.Key, entry.Value);
				}

				properties.Store(fileStream, null);
			} finally {
				if (fileStream != null)
					fileStream.Close();
			}
			*/

			FileStream fileStream = null;

			try {
				fileStream = new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
				fileStream.SetLength(0);
				fileStream.Seek(0, SeekOrigin.Begin);

				SaveTo(fileStream);
			} finally {
				if (fileStream != null)
					fileStream.Close();
			}
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="stream"></param>
		public void SaveTo(Stream stream) {
			if (stream == null)
				throw new ArgumentNullException("stream");
			if (!stream.CanWrite)
				throw new ArgumentException("The stream is not writeable.");

			StreamWriter writer = new StreamWriter(stream, Encoding.GetEncoding("ISO-8859-1"));
			writer.WriteLine("#" + DateTime.Now);

			StringBuilder s = new StringBuilder(); // Reuse the same buffer.
			foreach (DictionaryEntry entry in key_map) {
				ConfigProperty property = (ConfigProperty) entry.Value;

				//TODO: format and print the optional comment...

				FormatForOutput(property.Key, s, true);
				s.Append('=');
				FormatForOutput(property.Value, s, false);
				writer.WriteLine(s);
			}

			writer.Flush();
		}

		public static DbConfig CreateDefault() {
			DbConfig config = new DbConfig();
			config.SetValue("storage_system", "v1heap");
			config.SetValue("database_path", "./data");
			config.SetValue("log_path", "./log");
			/*
			Moved out of the kernel...
			config.SetValue("server_port", "9157");
			config.SetValue("server_address", "127.0.0.1");
			*/
			config.SetValue("ignore_case_for_identifiers", "disabled");
			config.SetValue("regex_library", "Deveel.Text.DeveelRegexLibrary");
			config.SetValue("data_cache_size", "4194304");
			config.SetValue("max_cache_entry_size", "8192");
			config.SetValue("lookup_comparison_list", "enabled");
			config.SetValue("maximum_worker_threads", "4");
			config.SetValue("dont_synch_filesystem", "disabled");
			config.SetValue("transaction_error_on_dirty_select", "enabled");
			config.SetValue("read_only", "disabled");
			config.SetValue("debug_log_file", "debug.log");
			config.SetValue("debug_level", "20");
			config.SetValue("table_lock_check", "enabled");
			return config;
		}
	}
}