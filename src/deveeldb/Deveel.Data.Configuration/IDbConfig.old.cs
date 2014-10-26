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
using System.Collections.Generic;

namespace Deveel.Data.Configuration {
	/// <summary>
	/// Defines the basic contract for a configuration provider
	/// </summary>
	/// <remarks>
	/// <para>
	/// Implementations of this interface will provide a hieratical access to
	/// configuration elements within a system: the materialized format is
	/// delegated to implementations of<see cref="IConfigFormatter"/>.
	/// </para>
	/// <para>
	/// A <see cref="IDbConfig"/> acts like the node of a tree, which can have
	/// a <see cref="Parent"/> and one or many children.
	/// </para>
	/// <para>
	/// By default a call to <see cref="GetValue"/> provides a values on the same level 
	/// of the node. Using a dot-separated key name will resolve the children of the object.
	/// </para>
	/// </remarks>
	public interface IDbConfig : IEnumerable<KeyValuePair<string, object>>, ICloneable {
		/// <summary>
		/// Gets or sets the source of the configurations node.
		/// </summary>
		IConfigSource Source { get; set; }

		/// <summary>
		/// Gets or sets the parent configuration, that handles this object.
		/// </summary>
		IDbConfig Parent { get; set; }

		/// <summary>
		/// Gets a key/value pair of this configuration or the children
		/// </summary>
		/// <param name="level">The level of configrations to return.</param>
		/// <returns>
		/// Returns a set of configurations for this configuration or the
		/// children, if <paramref name="level"/> is set to <see cref="ConfigurationLevel.Deep"/>.
		/// </returns>
		IEnumerable<KeyValuePair<string, object>> GetLevel(ConfigurationLevel level);
		
		/// <summary>
		/// Gets a configuration value for the give key, or the value
		/// specified, if the configuration was not found.
		/// </summary>
		/// <param name="key">The key to find the configuration value to return.</param>
		/// <param name="defaultValue">The value to return if none configuration
		/// was found for the given key.</param>
		/// <returns>
		/// Returns a configuration value associated to the given key, or the
		/// given configuration was not found.
		/// </returns>
		object GetValue(string key, object defaultValue);

		/// <summary>
		/// Sets the configuration value associated to the given key.
		/// </summary>
		/// <param name="key">The key of the configuration to set.</param>
		/// <param name="value">The value to associate to the given key.</param>
		void SetValue(string key, object value);
	}
}
