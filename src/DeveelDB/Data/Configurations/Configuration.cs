// 
//  Copyright 2010-2018 Deveel
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
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data;

namespace Deveel.Data.Configurations {
	public class Configuration : IConfiguration {
		private readonly Dictionary<string, object> values;
		private readonly Dictionary<string, IConfiguration> childConfigurations;

		/// <summary>
		/// A character that separates sections in a configuration context
		/// </summary>
		public const char SectionSeparator = '.';

		/// <summary>
		/// Constructs the <see cref="Configuration"/>.
		/// </summary>
		public Configuration() {
			values = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
			childConfigurations = new Dictionary<string, IConfiguration>(StringComparer.OrdinalIgnoreCase);
		}

		/// <inheritdoc/>
		public IConfiguration Parent { get; set; }

		/// <inheritdoc/>
		public IEnumerable<string> Keys {
			get { return values.Keys; }
		}

		/// <inheritdoc/>
		public IEnumerator<ConfigurationValue> GetEnumerator() {
			return values.Select(x => new ConfigurationValue(x.Key, x.Value)).GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}


		/// <summary>
		/// Sets a given value for a key defined by this object.
		/// </summary>
		/// <param name="key">The key to set the value for, that was defined before.</param>
		/// <param name="value">The value to set.</param>
		/// <remarks>
		/// <para>
		/// If the given <paramref name="key"/> was not previously defined,
		/// this method will add the key at this level of configuration
		/// </para>
		/// <para>
		/// Setting a value for a given <paramref name="key"/> that was already
		/// defined by a parent object will override that value: a subsequent call
		/// to <see cref="GetValue"/> will return the current value of the setting,
		/// without removing the parent value setting.
		/// </para>
		/// <para>
		/// If the key is formed to reference a child section, the value is
		/// set to the key parented by the referenced section.
		/// </para>
		/// </remarks>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="key"/> is <c>null</c>.
		/// </exception>
		/// <seealso cref="SectionSeparator"/>
		public void SetValue(string key, object value) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException(nameof(key));

			var parts = key.Split(SectionSeparator);
			if (parts.Length == 0)
				throw new ArgumentException();

			if (parts.Length == 1) {
				if (value == null) {
					values.Remove(key);
				} else {
					values[key] = value;
				}
			} else {
				Configuration config = this;
				for (int i = 0; i < parts.Length; i++) {
					var part = parts[i];
					if (i == parts.Length - 1) {
						config.SetValue(part, value);
						return;
					}

					var child = config.GetChild(part);
					if (child == null) {
						child = new Configuration();
						config.AddSection(part, child);
					} else if (!(child is Configuration)) {
						throw new NotSupportedException();
					}

					config = (Configuration) child;
				}
			}
		}

		/// <inheritdoc/>
		public object GetValue(string key) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException(nameof(key));

			var parts = key.Split(SectionSeparator);
			if (parts.Length == 0)
				throw new ArgumentException();

			if (parts.Length == 1) {
				object value;
				if (values.TryGetValue(key, out value))
					return value;

				return null;
			}

			IConfiguration config = this;
			for (int i = 0; i < parts.Length; i++) {
				var part = parts[i];
				if (i == parts.Length - 1)
					return config.GetValue(part);

				config = config.GetChild(part);

				if (config == null)
					return null;
			}

			return null;
		}

		/// <summary>
		/// Adds a child configuration to this one
		/// </summary>
		/// <param name="key">The key used to identify the child configuration</param>
		/// <param name="configuration">The configuration object</param>
		public void AddSection(string key, IConfiguration configuration) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException(nameof(key));
			if (configuration == null)
				throw new ArgumentNullException(nameof(configuration));

			childConfigurations[key] = configuration;
		}

		/// <inheritdoc cref="IConfiguration.Sections"/>
		public IEnumerable<KeyValuePair<string, IConfiguration>> Sections {
			get { return childConfigurations.AsEnumerable(); }
		}

		public static IConfiguration Build(Action<IConfigurationBuilder> config) {
			var builder = new ConfigurationBuilder();
			config(builder);
			return builder.Build();
		}

		public static IConfigurationBuilder Builder() {
			return new ConfigurationBuilder();
		}
	}
}