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
using System.Globalization;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Describes the properties of a schema in a database system.
	/// </summary>
	/// <remarks>
	/// <para>
	/// A schema is a collection of database objects (for example
	/// <c>TABLE</c>, <c>VIEW</c>, <c>TYPE</c>, <c>TRIEGGER</c>, etc.).
	/// </para>
	/// <para>
	/// It is possible for a schema to specify additional metadata
	/// information, such as the culture that will be used by default
	/// to collate strings in comparisons.
	/// </para>
	/// <para>
	/// It is not possible to define more than one schema with the same
	/// name in a database.
	/// </para>
	/// </remarks>
	public sealed class SchemaInfo : IObjectInfo {
		private CompareInfo comparer;

		/// <summary>
		/// Constructs the schema with the given name
		/// </summary>
		/// <param name="name">The name that identifies the schema.</param>
		/// <param name="type">The type of the schema to create.</param>
		public SchemaInfo(string name, string type) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			Name = name;
			Type = type;
		}

		DbObjectType IObjectInfo.ObjectType {
			get { return DbObjectType.Schema; }
		}


		ObjectName IObjectInfo.FullName {
			get { return new ObjectName(Name); }
		}

		/// <summary>
		/// Gets the name of the schema.
		/// </summary>
		public string Name { get; private set; }

		/// <summary>
		/// Gets the type of the schema that defines the kind of objects it
		/// can contains.
		/// </summary>
		public string Type { get; private set; }

		/// <summary>
		/// Gets the culture that will be applied to string
		/// comparisons, when not explicitly defined by types.
		/// </summary>
		public string Culture { get; set; }

		/// <summary>
		/// Compares two strngs given using the culture set in the schema.
		/// </summary>
		/// <param name="s1">The first string to compare.</param>
		/// <param name="s2">The second argument of the comparison.</param>
		/// <returns>
		/// Returns an integer value of 1 if the fist string is greather than the second,
		/// -1 if the second string is greather than the first one, or 0 if the
		/// two strings are equal.
		/// </returns>
		/// <seealso cref="Compare(string, string, bool)"/>
		public int Compare(string s1, string s2) {
			return Compare(s1, s2, false);
		}

		/// <summary>
		/// Compares two strngs given using the culture set in the schema.
		/// </summary>
		/// <param name="s1">The first string to compare.</param>
		/// <param name="s2">The second argument of the comparison.</param>
		/// <param name="ignoreCase">Indicates whether to ignore the case of strings or not.</param>
		/// <remarks>
		/// If the <see cref="Culture"/> metadata was not set in the schema, the
		/// two strings will be compared using the invariant culture.
		/// </remarks>
		/// <returns>
		/// Returns an integer value of 1 if the fist string is greather than the second,
		/// -1 if the second string is greather than the first one, or 0 if the
		/// two strings are equal.
		/// </returns>
		public int Compare(string s1, string s2, bool ignoreCase) {
			var options = ignoreCase ? CompareOptions.OrdinalIgnoreCase : CompareOptions.Ordinal;

			if (String.IsNullOrEmpty(Culture))
				return CultureInfo.InvariantCulture.CompareInfo.Compare(s1, s2, options);

			if (comparer == null)
				comparer = new CultureInfo(Culture).CompareInfo;

			return comparer.Compare(s1, s2, options);
		}
	}
}
