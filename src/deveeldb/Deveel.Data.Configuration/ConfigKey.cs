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

namespace Deveel.Data.Configuration {
	/// <summary>
	/// A single key of a configuration that defines the name and type
	/// of the value that a <see cref="ConfigValue"/> associated
	/// with this key will handle.
	/// </summary>
	/// <remarks>
	/// <para>
	/// A key is unique within a configuration object and when defined
	/// by a child in the configuration three, this will replace the one
	/// defined in the parent.
	/// </para>
	/// <para>
	/// Keys can be aggregated by groups using a specific <see cref="ConfigGroup.Separator"/>.
	/// </para>
	/// </remarks>
	/// <seealso cref="ConfigValue"/>
	/// <seealso cref="ConfigGroup.Separator"/>
	public sealed class ConfigKey {
		/// <summary>
		/// Constructs the key with the given name and value type.
		/// </summary>
		/// <param name="name">The unique name of the configuration key.</param>
		/// <param name="valueType">The type of compatible values this key will handle.</param>
		/// <remarks>
		/// This constructor does not define any <c>default value</c> for
		/// missing values in a configuration, that means a request for
		/// the key will return the <see cref="ValueType"/> default.
		/// </remarks>
		public ConfigKey(string name, Type valueType) 
			: this(name, null, valueType) {
		}

		/// <summary>
		/// Constructs the key with the given name, value type and a provided
		/// default value.
		/// </summary>
		/// <param name="name">The unique name of the configuration key.</param>
		/// <param name="valueType">The type of compatible values this key will handle.</param>
		/// <param name="defaultValue">A given default value that will be
		/// returned when a value is not explicitly associated to this key.</param>
		public ConfigKey(string name, object defaultValue, Type valueType) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");
			if (valueType == null)
				throw new ArgumentNullException("valueType");

			ValueType = valueType;
			Name = name;
			DefaultValue = defaultValue;
		}

		/// <summary>
		/// Gets the key name that is unique within the configuration context.
		/// </summary>
		public string Name { get; private set; }

		/// <summary>
		/// Gets the <see cref="Type"/> of the value handled by this key.
		/// </summary>
		public Type ValueType { get; private set; }

		/// <summary>
		/// Gets an optional default value that will be returned
		/// in case of missing explicitly set associated value.
		/// </summary>
		public object DefaultValue { get; private set; }
	}
}