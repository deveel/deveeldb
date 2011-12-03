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
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;

namespace Deveel.Data.Control {
	/// <summary>
	///  Provides configurations for the whole database system.
	/// </summary>
	public sealed class DbConfig : IEnumerable<KeyValuePair<string, string>> {
		/// <summary>
		/// The Hashtable mapping from configuration key to value for the key.
		/// </summary>
		private Dictionary<string, string> properties;

		private static DbConfig defaultConfig;

		/// <summary>
		/// Constructs the <see cref="DbConfig"/>.
		/// </summary>
		public DbConfig() {
			properties = new Dictionary<string, string>();
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
				return (value == "enabled" || value == "true");
			}
			set { SetValue(ConfigKeys.IgnoreIdentifiersCase, value ? "true" : "false"); }
		}

		/// <summary>
		/// Gets or sets that the database is read-only.
		/// </summary>
		public bool ReadOnly {
			get {
				string value = GetValue(ConfigKeys.ReadOnly);
				return (value == "enabled" || value == "true");
			}
			set { SetValue(ConfigKeys.ReadOnly, value ? "true" : "false"); }
		}

		/// <summary>
		/// Gets or sets the minimum debug level for output to the debug log file.
		/// </summary>
		public int DebugLevel {
			get {
				string value = GetValue(ConfigKeys.DebugLevel);
				return (value == null ? -1 : Int32.Parse(value));
			}
			set { SetValue(ConfigKeys.DebugLevel, value.ToString()); }
		}

		public string DebugLogFile {
			get { return GetStringValue(ConfigKeys.DebugLogFile, null); }
			set { SetValue(ConfigKeys.DebugLogFile, value); }
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

		///<summary>
		/// Sets the configuration value for the key property key.
		///</summary>
		///<param name="key"></param>
		///<param name="value"></param>
		public void SetValue(string key, string value) {
			properties[key] = value;
		}

		public void SetValue(string propertyKey, bool value) {
			properties[propertyKey] = (value ? "enabled" : "disabled");
		}

		public void SetValue(string propertyKey, int value) {
			properties[propertyKey] = value.ToString();
		}

		// ---------- Implemented from IDbConfig ----------

		public string CurrentPath {
			get { 
				string value = GetValue(ConfigKeys.BasePath);
				return value ?? ".";
			}
			set { SetValue(ConfigKeys.BasePath, value); }
		}

		/// <summary>
		/// Returns a <see cref="DbConfig">default</see> implementation
		/// of <see cref="DbConfig"/> which contains all the default settings
		/// of the system.
		/// </summary>
		public static DbConfig Default {
			get { return defaultConfig ?? (defaultConfig = CreateDefault()); }
		}

		public string GetValue(string propertyKey) {
			return GetValue(propertyKey, null);
		}

		public string GetValue(string propertyKey, string defaultValue) {
			// If the key is in the map, return it here
			string property;
			if (!properties.TryGetValue(propertyKey, out property))
				return defaultValue;
			return property;
		}

		public bool GetBooleanValue(string propertyKey, bool defaultValue) {
			String v = GetValue(propertyKey);
			return v == null ? defaultValue : String.Compare(v.Trim(), "enabled", true) == 0;
		}

		public string GetStringValue(string propertyKey, string defaultValue) {
			String v = GetValue(propertyKey);
			return v == null ? defaultValue : v.Trim();
		}

		public int GetIntegerValue(string property, int defaultValue) {
			String v = GetValue(property);
			return v == null ? defaultValue : Int32.Parse(v);
		}

		/// <summary>
		/// Parses a file string to an absolute position in the file system.
		/// </summary>
		/// <remarks>
		/// We must provide the path to the root directory (eg. the directory 
		/// where the config bundle is located).
		/// </remarks>
		public string ParseFileString(string rootInfo, string pathString) {
			string path = Path.GetFullPath(pathString);
			string res;
			// If the path is absolute then return the absoluate reference
			if (Path.IsPathRooted(pathString)) {
				res = path;
			} else {
				// If the root path source is the environment then just return the path.
				if (rootInfo != null && rootInfo.Equals("env"))
					return path;

				// If the root path source is the configuration file then
				// concat the configuration path with the path string and return it.

				res = Path.Combine(CurrentPath, pathString);
			}

			return res;
		}


		/// <inheritdoc/>
		public DbConfig Merge(DbConfig config) {
			foreach (KeyValuePair<string, string> property in config.properties) {
				if (properties.ContainsKey(property.Key))
					continue;

				properties[property.Key] = (string) property.Value.Clone();
			}

			return this;
		}

		/// <inheritdoc/>
		public object Clone() {
			DbConfig config = new DbConfig();
			config.properties = new Dictionary<string, string>();
			foreach (KeyValuePair<string, string> pair in properties) {
				config.properties[pair.Key] = (string) pair.Value.Clone();
			}
			return config;
		}

		IEnumerator<KeyValuePair<string, string>> IEnumerable<KeyValuePair<string, string>>.GetEnumerator() {
			return properties.GetEnumerator();
		}

		/// <summary>
		/// Gets an anumeration of all the key/value pairs set 
		/// in this object. 
		/// </summary>
		/// <returns>
		/// Returns an instance of <see cref="IEnumerator"/> which is used
		/// to enumerate a set of <see cref="KeyValuePair{TKey,TValue}"/> which represent
		/// the configuration properties set.
		/// </returns>
		public IEnumerator GetEnumerator() {
			return properties.GetEnumerator();
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
							line = reader.ReadLine() ?? "";
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
						Char.IsWhiteSpace(line[pos]))
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
								   && Char.IsWhiteSpace(line[pos]))
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
				foreach (ConfigProperty entry in properties.Values) {
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
			foreach (KeyValuePair<string, string> entry in properties) {
				//TODO: format and print the optional comment...

				FormatForOutput(entry.Key, s, true);
				s.Append('=');
				FormatForOutput(entry.Value, s, false);
				writer.WriteLine(s);
			}

			writer.Flush();
		}

		public static DbConfig CreateDefault() {
			DbConfig config = new DbConfig();
			config.SetValue(ConfigKeys.StorageSystem, "v1heap");
			config.SetValue(ConfigKeys.DatabasePath, "./data");
			config.SetValue(ConfigKeys.LogPath, "./log");
			/*
			Moved out of the kernel...
			config.SetValue("server_port", "9157");
			config.SetValue("server_address", "127.0.0.1");
			*/
			config.SetValue(ConfigKeys.IgnoreIdentifiersCase, "disabled");
			config.SetValue(ConfigKeys.RegexLibrary, "Deveel.Text.DeveelRegexLibrary");
			config.SetValue(ConfigKeys.DataCacheSize, "4194304");
			config.SetValue(ConfigKeys.MaxCacheEntrySize, "8192");
			config.SetValue(ConfigKeys.LookupComparisonList, "enabled");
			config.SetValue(ConfigKeys.MaxWorkerThreads, "4");
			//config.SetValue("dont_synch_filesystem", "disabled");
			config.SetValue(ConfigKeys.TransactionErrorOnDirtySelect, "enabled");
			config.SetValue(ConfigKeys.ReadOnly, "disabled");
			config.SetValue(ConfigKeys.DebugLogFile, "debug.log");
			config.SetValue(ConfigKeys.DebugLevel, "20");
			config.SetValue(ConfigKeys.TableLockCheck, "enabled");
			return config;
		}
	}
}