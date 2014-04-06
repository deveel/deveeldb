// 
//  Copyright 2010-2014  Deveel
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
using System.Globalization;
using System.IO;
using System.Net;

using Deveel.Data.Caching;

namespace Deveel.Data.Configuration {
	/// <summary>
	///  Provides configurations for the whole database system.
	/// </summary>
	public class DbConfig : IDbConfig {
		/// <summary>
		/// The Hashtable mapping from configuration key to value for the key.
		/// </summary>
		private Dictionary<string, object> properties;

		//private static DbConfig defaultConfig;

		public const int DefaultDataCacheSize = 256;

		/// <summary>
		/// Constructs the <see cref="DbConfig"/>.
		/// </summary>
		public DbConfig() {
			properties = new Dictionary<string, object>();
		}

		/// <summary>
		/// Constructs the <see cref="DbConfig"/> from the current parent.
		/// </summary>
		/// <param name="parent">The parent <see cref="DbConfig"/> object that
		/// will provide fallback configurations</param>
		/// <param name="source"></param>
		public DbConfig(IDbConfig parent, IConfigSource source)
			: this() {
			Parent = parent;
			Source = source;

			if (source != null)
				this.Load(source);
		}

		public DbConfig(IConfigSource source)
			: this(null, source) {
		}

		public DbConfig(IDbConfig parent)
			: this(parent, null) {
		}

		/// <summary>
		/// Gets or sets the parent set of conigurations
		/// </summary>
		public IDbConfig Parent { get; set; }

		public IConfigSource Source { get; set; }

		public IEnumerable<string> Keys {
			get { return properties.Keys; }
		}

		/// <summary>
		/// Gets or sets the path to the database.
		/// </summary>
		public string DatabasePath {
			set { SetValue(ConfigKeys.DatabasePath, value); }
			get { return GetValue<string>(ConfigKeys.DatabasePath); }
		}

		///<summary>
		/// Gets or sets the path of the log.
		///</summary>
		public string LogPath {
			get { return GetValue<string>(ConfigKeys.LogPath); }
			set { SetValue(ConfigKeys.LogPath, value); }
		}

		/// <summary>
		/// Gets or sets that the engine ignores case for identifiers.
		/// </summary>
		public bool IgnoreIdentifierCase {
			get { return GetValue<bool>(ConfigKeys.IgnoreIdentifiersCase); }
			set { SetValue(ConfigKeys.IgnoreIdentifiersCase, value); }
		}

		/// <summary>
		/// Gets or sets that the database is read-only.
		/// </summary>
		public bool ReadOnly {
			get { return GetValue<bool>(ConfigKeys.ReadOnly); }
			set { SetValue(ConfigKeys.ReadOnly, value); }
		}

		public string DebugLogFile {
			get { return GetValue<string>(ConfigKeys.DebugLogFile, null); }
			set { SetValue(ConfigKeys.DebugLogFile, value); }
		}

		public string BasePath {
			get { return GetValue(ConfigKeys.BasePath, "."); }
			set { SetValue(ConfigKeys.BasePath, value); }
		}

		public string StorageSystem {
			get { return GetValue(ConfigKeys.StorageSystem, ConfigValues.HeapStorageSystem); }
			set { SetValue(ConfigKeys.StorageSystem, value); }
		}

		public Type CacheType {
			get {
				var typeString = GetValue<string>(ConfigKeys.CacheType, ConfigValues.HeapCache);
				if (String.IsNullOrEmpty(typeString))
					return null;

				if (String.Equals(typeString, ConfigValues.HeapCache, StringComparison.OrdinalIgnoreCase))
					return typeof (MemoryCache);

				return Type.GetType(typeString, false, true);
			}
			set {
				string typeString = null;
				if (value != null)
					typeString = value.AssemblyQualifiedName;
				SetValue(ConfigKeys.CacheType, typeString);
			}
		}

		public int DataCacheSize {
			get { return GetValue<int>(ConfigKeys.DataCacheSize, DefaultDataCacheSize); }
			set { SetValue(ConfigKeys.DataCacheSize, value); }
		}

		// TODO: make the default config
		public int MaxCacheEntrySize {
			get { return GetValue(ConfigKeys.MaxCacheEntrySize, 128); }
			set { SetValue(ConfigKeys.MaxCacheEntrySize, value); }
		}

		///<summary>
		/// Sets the configuration value for the key property key.
		///</summary>
		///<param name="key"></param>
		///<param name="value"></param>
		public void SetValue<T>(string key, T value) {
			SetValue(key, (object)value);
		}

		public void SetValue(string key, object value) {
			properties[key] = value;
		}

		public object GetValue(string propertyKey) {
			return GetValue(propertyKey, null);
		}

		public object GetValue(string propertyKey, object defaultValue) {
			// If the key is in the map, return it here
			object property;
			if (properties.TryGetValue(propertyKey, out property))
				return property;
			if (Parent == null)
				return defaultValue;
			if ((property = Parent.GetValue(propertyKey, null)) != null)
				return property;

			return defaultValue;
		}

		public T GetValue<T>(string propertyKey) {
			object value = GetValue(propertyKey);
			if (value == null || Equals(default(T), value))
				return default(T);
			if (!(value is T))
				value = Convert.ChangeType(value, typeof (T), CultureInfo.InvariantCulture);

			return (T) value;
		}

		public T GetValue<T>(string propertyKey, T defaultValue) {
			T value;

			try {
				value = GetValue<T>(propertyKey);
			} catch (Exception) {
				return defaultValue;
			}
			
			if (Equals(default(T), value))
				return defaultValue;

			return value;
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

				res = Path.Combine(BasePath, pathString);
			}

			return res;
		}

		/// <inheritdoc/>
		public object Clone() {
			DbConfig parentClone = null;
			if (Parent != null)
				parentClone = (DbConfig) Parent.Clone();

			DbConfig config = new DbConfig(parentClone);
			config.properties = new Dictionary<string, object>();
			foreach (KeyValuePair<string, object> pair in properties) {
				object value = pair.Value;
				if (value != null && value is ICloneable)
					value = ((ICloneable) value).Clone();

				config.properties[pair.Key] = value;
			}
			return config;
		}

		public IEnumerator<KeyValuePair<string, object>> GetEnumerator() {
			return new Enumerator(properties);
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
		IEnumerator IEnumerable.GetEnumerator() {
			return properties.GetEnumerator();
		}


		#region Enumerator

		class Enumerator : IEnumerator<KeyValuePair<string, object>> {
			private readonly Dictionary<string, object> properties;
			private Dictionary<string, object>.KeyCollection.Enumerator enumerator;

			public Enumerator(Dictionary<string, object> properties) {
				this.properties = properties;
				enumerator = properties.Keys.GetEnumerator();
			}

			public void Dispose() {
			}

			public bool MoveNext() {
				return enumerator.MoveNext();
			}

			public void Reset() {
				enumerator = properties.Keys.GetEnumerator();
			}

			public KeyValuePair<string, object> Current {
				get {
					string key = enumerator.Current;
					object value = properties[key];
					return new KeyValuePair<string, object>(key, value);
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}
		}

		#endregion
	}
}