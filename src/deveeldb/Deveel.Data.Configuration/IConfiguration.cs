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

namespace Deveel.Data.Configuration {
	/// <summary>
	/// Defines the contract for the configuration node of a component within
	/// the system or of the system itself.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Since the configurations can be structured in <c>nodes</c> of a tree,
	/// it is possible to define a <see cref="Parent"/>, that will be used
	/// to resolve a key and value of a setting, if the current node does not
	/// define it by itself.
	/// </para>
	/// </remarks>
	public interface IConfiguration : IEnumerable<KeyValuePair<string, object>> {
		/// <summary>
		/// Gets or sets an optional source of the configuration object
		/// </summary>
		/// <remarks>
		/// This property is optional and it is convenient when it is
		/// required to save or reload the values to/from the source, retaining
		/// a reference to the origin.
		/// </remarks>
		/// <seealso cref="IConfigSource"/>
		IConfigSource Source { get; set; }

		/// <summary>
		/// Gets or sets an optional parent object of this configuration.
		/// </summary>
		/// <remarks>
		/// <para>
		/// The tree structure of a configuration object makes the request
		/// for a given setting to fallback to parents if the object does
		/// not define the key itself.
		/// </para>
		/// <para>
		/// A typical example of a parent/child structure is the configuration
		/// of a database system and the configuration of a single database:
		/// the system defines default values for all the databases, while a
		/// single database can override a setting or define new settings.
		/// </para>
		/// </remarks>
		IConfiguration Parent { get; set; }

		/// <summary>
		/// Enumerates the keys that can be obtained by the object, at
		/// the given <see cref="ConfigurationLevel"/>.
		/// </summary>
		/// <param name="level">The level of definition of the keys to get.</param>
		/// <returns>
		/// Returns an enumeration of <see cref="string"/> representing the
		/// keys that are accessible from this object, depending on the level of 
		/// nesting given.
		/// </returns>
		/// <seealso cref="ConfigurationLevel"/>
		IEnumerable<string> GetKeys(ConfigurationLevel level);
		
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
		/// Setting a value for a given <see cref="key"/> that was already
		/// defined by a parent object will override that value: a subsequent call
		/// to <see cref="GetValue"/> will return the current value of the setting,
		/// without removing the parent value setting.
		/// </para>
		/// </remarks>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="key"/> is <c>null</c>.
		/// </exception>
		void SetValue(string key, object value);

		/// <summary>
		/// Gets a configuration setting for the given key.
		/// </summary>
		/// <param name="key">The key that identifies the setting to retrieve.</param>
		/// <returns>
		/// Returns a configuration value if defined by the provided key, or <c>null</c>
		/// if the key was not found in this or in the parent context.
		/// </returns>
		object GetValue(string key);
	}
}