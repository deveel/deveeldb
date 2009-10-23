//  
//  DbConfig.cs
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
using System.Globalization;
using System.IO;
using System.Net;

using Deveel.Data.Control;
using Deveel.Data.Util;

namespace Deveel.Data {
	/// <summary>
	///  An basic implementation of <see cref="IDbConfig"/>.
	/// </summary>
	public class DbConfig : IDbConfig {
		/// <summary>
		/// The current base path of the database configuration.
		/// </summary>
		private readonly string current_path;

		/// <summary>
		/// The Hashtable mapping from configuration key to value for the key.
		/// </summary>
		private Hashtable key_map;

		private static DefaultDbConfig default_config;

		/// <summary>
		/// Constructs the <see cref="IDbConfig"/>.
		/// </summary>
		/// <param name="current_path"></param>
		public DbConfig(string current_path) {
			this.current_path = current_path;
			key_map = new Hashtable();
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
		public void SetValue(String key, String value) {
			SetValue(key, value, "STRING");
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="value"></param>
		/// <param name="type"></param>
		public void SetValue(string key, string value, string type) {
			key_map[key] = new ConfigProperty(key, value, type);
		}

		// ---------- Implemented from IDbConfig ----------

		public string CurrentPath {
			get { return current_path; }
		}

		/// <summary>
		/// Returns a <see cref="DefaultDbConfig">default</see> implementation
		/// of <see cref="IDbConfig"/> which contains all the default settings
		/// of the system.
		/// </summary>
		public static DefaultDbConfig Default {
			get {
				if (default_config == null)
					default_config = new DefaultDbConfig();
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
			DbConfig config = new DbConfig(current_path);
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

		public virtual void SaveTo(string fileName) {
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
		}

		/// <summary>
		/// Saves the current configurations to the current path
		/// and to the default file name.
		/// </summary>
		public void SaveTo() {
			SaveTo(Path.Combine(current_path, "db.conf"));
		}
	}
}