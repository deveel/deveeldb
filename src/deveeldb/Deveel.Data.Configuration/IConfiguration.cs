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
	/// Configuration objects are structured in <see cref="ConfigKey">keys</see>
	/// that define the name and type of the setting they handle. An association
	/// to <see cref="ConfigValue">values</see> make the key to return a setting
	/// when defined.
	/// </para>
	/// <para>
	/// Since the configurations can be structured in <c>nodes</c> of a tree,
	/// it is possible to define a <see cref="Parent"/>, that will be used
	/// to resolve a key and value of a setting, if the current node does not
	/// define it by itself.
	/// </para>
	/// </remarks>
	public interface IConfiguration {
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
		/// Returns an enumeration of the <see cref="ConfigKey"/> that are
		/// accessible from this object, depending on the level of nesting given.
		/// </returns>
		/// <seealso cref="ConfigurationLevel"/>
		/// <seealso cref="ConfigKey"/>
		IEnumerable<ConfigKey> GetKeys(ConfigurationLevel level);
		
		/// <summary>
		/// Otains a key identified by the given name that is defined
		/// by this object or by the tree.
		/// </summary>
		/// <param name="name">The name of the key to obtain.</param>
		/// <remarks>
		/// <para>
		/// Key names are case-sensitive and are defined in simple text forms.
		/// </para>
		/// <para>
		/// Best practice for defining key names in configurations is to group
		/// them by <see cref="ConfigGroup.Separator"/>, that will make the default
		/// extensions to return a group of configurations per component.
		/// </para>
		/// <para>
		/// The configuration key will be resolved in a tree-based accessed
		/// logic, which does not ensure the key will be obtained by this object,
		/// but instead it will ascend all the parent nodes to find the one matching
		/// the given name.
		/// </para>
		/// <para>
		/// When setting a new key on this object, the resolved key obtained by this
		/// method will be the one defined at the current level, even if a parent
		/// already defined one with the same name.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		/// <see cref="ConfigKey"/>
		/// <seealso cref="ConfigKey.Name"/>
		/// <seealso cref="SetKey"/>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="name"/> is <c>null</c> or empty.
		/// </exception>
		ConfigKey GetKey(string name);

		/// <summary>
		/// Sets a key at the current level of configuration.
		/// </summary>
		/// <param name="key">The key to set on the configuration object.</param>
		/// <remarks>
		/// <para>
		/// If a key with the given <see cref="ConfigKey.Name"/> was already
		/// defined at this level or a parent level, this method will not throw
		/// any exception and will eventually redefine on the object
		/// </para>
		/// <para>
		/// Setting a key to the configuration object has the effect to define
		/// a default value associated to the setting identified by the key: to set 
		/// the value of a setting a call to <see cref="SetValue"/> must be issued.
		/// </para>
		/// <para>
		/// </para>
		/// </remarks>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="key"/> object is <c>null</c>.
		/// </exception>
		/// <seealso cref="SetValue"/>
		void SetKey(ConfigKey key);

		/// <summary>
		/// Sets a given value for a key defined by this object.
		/// </summary>
		/// <param name="key">The key to set the value for, that was defined before.</param>
		/// <param name="value">The value to set.</param>
		/// <remarks>
		/// <para>
		/// If the given <paramref name="key"/> was not previously defined by <see cref="SetKey"/>,
		/// this method will add the key at this level of configuration
		/// </para>
		/// <para>
		/// Setting a value for a given <see cref="ConfigKey"/> that was already
		/// defined by a parent object will override that value: a subsequent call
		/// to <see cref="GetValue"/> will return the current value of the setting,
		/// without removing the parent value setting.
		/// </para>
		/// </remarks>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="key"/> is <c>null</c>.
		/// </exception>
		void SetValue(ConfigKey key, object value);

		/// <summary>
		/// Gets a configuration setting for the given key.
		/// </summary>
		/// <param name="key">The key that identifies the setting to retrieve.</param>
		/// <returns>
		/// Returns a configuration value if defined by the provided key, or the
		/// <seealso cref="ConfigKey.DefaultValue"/> if the key was set.
		/// </returns>
		ConfigValue GetValue(ConfigKey key);
	}
}