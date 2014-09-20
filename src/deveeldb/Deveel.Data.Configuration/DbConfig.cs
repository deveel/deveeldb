// 
//  Copyright 2010-2014 Deveel
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

namespace Deveel.Data.Configuration {
	/// <summary>
	///  Provides configurations for the whole database system.
	/// </summary>
	public class DbConfig : IDbConfig {
		/// <summary>
		/// The Hashtable mapping from configuration key to value for the key.
		/// </summary>
		private Dictionary<string, object> properties;

		public const int DefaultDataCacheSize = 256;

		private readonly bool isRoot;

		/// <summary>
		/// Constructs the <see cref="DbConfig"/>.
		/// </summary>
		private DbConfig(bool isRoot) {
			Parent = null;
			this.isRoot = isRoot;
			properties = new Dictionary<string, object>();
		}

		/// <summary>
		/// Constructs the <see cref="DbConfig"/> from the current parent.
		/// </summary>
		/// <param name="parent">The parent <see cref="DbConfig"/> object that
		/// will provide fallback configurations</param>
		/// <param name="source"></param>
		public DbConfig(IDbConfig parent, IConfigSource source)
			: this(false) {
			this.Parent = parent;
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

		static DbConfig() {
			Default = new DbConfig(true) {
				properties = new Dictionary<string, object> {
					{ConfigKeys.BasePath, ConfigDefaultValues.BasePath},
					{ConfigKeys.CacheType, ConfigDefaultValues.HeapCache},
					{ConfigKeys.StorageSystem, ConfigDefaultValues.HeapStorageSystem},
					{ConfigKeys.DefaultSchema, ConfigDefaultValues.DefaultSchema},
					{ConfigKeys.IgnoreIdentifiersCase, ConfigDefaultValues.IgnoreIdentifiersCase},
					{ConfigKeys.CacheStatements, ConfigDefaultValues.CacheStatements},
					{ConfigKeys.MaxWorkerThreads, ConfigDefaultValues.MaxWorkerThreads},
					{ConfigKeys.PasswordHashFunction, ConfigDefaultValues.PasswordHashFunction},
					{ConfigKeys.ReadOnly, ConfigDefaultValues.ReadOnly}
				}
			};

			Empty = new DbConfig(true);
		}

		/// <summary>
		/// Gets or sets the parent set of conigurations
		/// </summary>
		public IDbConfig Parent { get; set; }

		public IConfigSource Source { get; set; }

		public static DbConfig Default { get; private set; }

		public static DbConfig Empty { get; private set; }

		public void SetValue(string key, object value) {
			properties[key] = value;
		}

		public object GetValue(string propertyKey, object defaultValue) {
			// If the key is in the map, return it here
			object property;
			if (properties.TryGetValue(propertyKey, out property))
				return property;

			if (!isRoot && Parent != null && 
				(property = Parent.GetValue(propertyKey, null)) != null)
				return property;

			return defaultValue;
		}

		/// <inheritdoc/>
		public object Clone() {
			//DbConfig parentClone = null;
			//if (parent != null)
			//	parentClone = (DbConfig) parent.Clone();
			if (isRoot)
				return this;

			var config = new DbConfig(false) {
				properties = new Dictionary<string, object>()
			};
			foreach (KeyValuePair<string, object> pair in properties) {
				object value = pair.Value;
				if (value is ICloneable)
					value = ((ICloneable) value).Clone();

				config.properties[pair.Key] = value;
			}

			if (Parent != null)
				config.Parent = (IDbConfig) Parent.Clone();

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