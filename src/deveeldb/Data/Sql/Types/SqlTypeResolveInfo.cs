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

namespace Deveel.Data.Sql.Types {
	/// <summary>
	/// Contains the information used to resolve a SQL type.
	/// </summary>
	public sealed class SqlTypeResolveInfo {
		/// <summary>
		/// Constructs an instance of <see cref="SqlTypeResolveInfo"/> with the
		/// given name of the type and an optional set of properties
		/// </summary>
		/// <param name="typeName">The name of the type to resolve</param>
		/// <param name="properties">An optional set of properties for the type.</param>
		/// <exception cref="ArgumentNullException">If the given <paramref name="typeName"/>
		/// is <c>null</c> or an empty string.</exception>
		public SqlTypeResolveInfo(string typeName, IDictionary<string, object> properties) {
			if (String.IsNullOrEmpty(typeName))
				throw new ArgumentNullException(nameof(typeName));

			if (properties == null)
				properties = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

			TypeName = typeName;
			Properties = properties;
		}

		/// <summary>
		/// Constructs an instance of <see cref="SqlTypeResolveInfo"/> with the
		/// given name of the type.
		/// </summary>
		/// <param name="typeName">The name of the type to resolve</param>
		/// <exception cref="ArgumentNullException">If the given <paramref name="typeName"/>
		/// is <c>null</c> or an empty string.</exception>
		public SqlTypeResolveInfo(string typeName)
			: this(typeName, new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)) {
		}

		/// <summary>
		/// Gets the name of the type to resolve
		/// </summary>
		public string TypeName { get; }

		/// <summary>
		/// Gets a set of properties of the type to resolve.
		/// </summary>
		public IDictionary<string, object> Properties { get; }
	}
}