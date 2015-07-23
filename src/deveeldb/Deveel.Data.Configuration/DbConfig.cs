// 
//  Copyright 2010-2015 Deveel
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
//

using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Configuration {
	[Serializable]
	public class DbConfig : IDbConfig {
		private readonly bool isRoot;
		private readonly Dictionary<string, ConfigKey> keys;
		private readonly Dictionary<string, ConfigValue> values;

		/// <summary>
		/// Constructs the <see cref="DbConfig"/>.
		/// </summary>
		private DbConfig(bool isRoot) {
			Parent = null;
			this.isRoot = isRoot;
			keys = new Dictionary<string, ConfigKey>();
			values = new Dictionary<string, ConfigValue>();
		}

		/// <summary>
		/// Constructs the <see cref="DbConfig"/> from the given parent.
		/// </summary>
		/// <param name="parent">The parent <see cref="DbConfig"/> object that
		/// will provide fallback configurations</param>
		/// <param name="source"></param>
		public DbConfig(IDbConfig parent, IConfigSource source)
			: this(false) {
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

		static DbConfig() {
			Empty = new DbConfig(true);

			SystemDefault = new DbConfig(true);
			SystemConfigKeys.SetTo(SystemDefault);

			DatabaseDefault = new DbConfig(true);
			DatabaseConfigKeys.SetTo(DatabaseDefault);
		}

		/// <inheritdoc/>
		public IConfigSource Source { get; set; }

		/// <inheritdoc/>
		public IDbConfig Parent { get; set; }

		/// <summary>
		/// An empty configuration object, which does not contain any key nor value.
		/// </summary>
		public static DbConfig Empty { get; private set; }

		public static DbConfig SystemDefault { get; private set; }

		public static DbConfig DatabaseDefault { get; private set; }

		/// <inheritdoc/>
		public IEnumerable<ConfigKey> GetKeys(ConfigurationLevel level) {
			var returnKeys = new Dictionary<string, ConfigKey>();
			if (!isRoot && Parent != null && level == ConfigurationLevel.Deep) {
				var configKeys = Parent.GetKeys(level);
				foreach (var pair in configKeys) {
					returnKeys[pair.Name] = pair;
				}
			}

			foreach (var configKey in keys) {
				returnKeys[configKey.Key] = configKey.Value;
			}

			return returnKeys.Values.AsEnumerable();
		}

		/// <inheritdoc/>
		public ConfigKey GetKey(string name) {
			ConfigKey key;
			if (keys.TryGetValue(name, out key))
				return key;

			if (!isRoot && Parent != null && (key = Parent.GetKey(name)) != null)
				return key;

			return null;
		}

		/// <inheritdoc/>
		public void SetKey(ConfigKey key) {
			if (key == null)
				throw new ArgumentNullException("key");

			keys[key.Name] = key;
		}

		/// <inheritdoc/>
		public void SetValue(ConfigKey key, object value) {
			if (key == null)
				throw new ArgumentNullException("key");

			if (!keys.ContainsKey(key.Name))
				keys[key.Name] = key;

			values[key.Name] = new ConfigValue(key, value);
		}

		/// <inheritdoc/>
		public ConfigValue GetValue(ConfigKey key) {
			if (key == null)
				throw new ArgumentNullException("key");

			ConfigValue value;
			if (values.TryGetValue(key.Name, out value))
				return value;

			if (!isRoot && Parent != null && 
				((value = Parent.GetValue(key)) != null))
				return value;

			return new ConfigValue(key, key.DefaultValue);
		}
	}
}