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
using System.Collections.Generic;

namespace Deveel.Data.Configurations {
	/// <summary>
	/// Defines the contract for the configuration node of a component within
	/// the system or of the system itself.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Configurations can be structured in <c>nodes</c> of a tree,
	/// it is possible to define child sections to a parent, and a descending
	/// order will be used to resolve a key and value of a setting, if the current 
	/// node does not define it by itself.
	/// </para>
	/// </remarks>
	public interface IConfiguration : IEnumerable<ConfigurationValue> {
		/// <summary>
		/// Enumerates the keys that can be obtained by the object
		/// </summary>
		/// <returns>
		/// Returns an enumeration of <see cref="string"/> representing the
		/// keys that are accessible from this object
		/// </returns>
		IEnumerable<string> Keys { get; }

		/// <summary>
		/// Gets a list of child sections of this configuration object
		/// </summary>
		IEnumerable<KeyValuePair<string, IConfiguration>> Sections { get; }


		/// <summary>
		/// Gets a configuration setting for the given key.
		/// </summary>
		/// <param name="key">The key that identifies the setting to retrieve.</param>
		/// <remarks>
		/// <para>
		/// If the given key references a value contained in a child section, that
		/// value will be returned.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns a configuration value if defined by the provided key, or <c>null</c>
		/// if the key was not found in this configuration or in a child section.
		/// </returns>
		/// <seealso cref="Configuration.SectionSeparator"/>
		object GetValue(string key);
	}
}