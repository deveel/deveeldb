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

using Deveel.Data;

namespace Deveel.Data.Configuration {
	public class Configuration : IConfiguration {
		private readonly bool isRoot;
		private readonly Dictionary<string, object> values;

		/// <summary>
		/// Constructs the <see cref="Configuration"/>.
		/// </summary>
		private Configuration(bool isRoot) {
			Parent = null;
			this.isRoot = isRoot;
			values = new Dictionary<string, object>();
		}

		/// <summary>
		/// Constructs the <see cref="Configuration"/> from the given parent.
		/// </summary>
		/// <param name="parent">The parent <see cref="Configuration"/> object that
		/// will provide fallback configurations</param>
		/// <param name="source"></param>
		public Configuration(IConfiguration parent, IConfigSource source)
			: this(parent == null) {
			Parent = parent;
			Source = source;

			if (source != null)
				this.Load(source);
		}

		public Configuration(IConfigSource source)
			: this(null, source) {
		}

		public Configuration()
			: this(true) {
		}

		public Configuration(IConfiguration parent)
			: this(parent, null) {
		}

		/// <inheritdoc/>
		public IConfigSource Source { get; set; }

		/// <inheritdoc/>
		public IConfiguration Parent { get; set; }

		/// <inheritdoc/>
		public IEnumerable<string> GetKeys(ConfigurationLevel level) {
			var returnKeys = new Dictionary<string, string>();
			if (!isRoot && Parent != null && level == ConfigurationLevel.Deep) {
				var configKeys = Parent.GetKeys(level);
				foreach (var pair in configKeys) {
					returnKeys[pair] = pair;
				}
			}

			foreach (var configKey in values.Keys) {
				returnKeys[configKey] = configKey;
			}

			return returnKeys.Values.AsEnumerable();
		}


		/// <inheritdoc/>
		public void SetValue(string key, object value) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			if (value == null) {
				values.Remove(key);
			} else {
				values[key] = value;
			}
		}

		/// <inheritdoc/>
		public object GetValue(string key) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			object value;
			if (values.TryGetValue(key, out value))
				return value;

			if (!isRoot && Parent != null && 
				((value = Parent.GetValue(key)) != null))
				return value;

			return null;
		}
	}
}