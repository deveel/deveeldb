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
using System.Diagnostics;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Describes the name of an object within a database.
	/// </summary>
	/// <remarks>
	/// The name of an object is composed by multiple parts, depending on the level
	/// of nesting of the object this name references.
	/// <para>
	/// For example, a reference to a table will be composed by the name of
	/// the schema and the name of the table: <c>Schema.Table</c>, while a reference
	/// to a column will be composed by the name of the schema, the name of the parent
	/// table and the name of the column itself: <c>Schema.Table.Column</c>.
	/// </para>
	/// <para>
	/// Depending on the xecution context, parts of the name can be omitted and will
	/// be resolved at run-time.
	/// </para>
	/// </remarks>
	[DebuggerDisplay("{FullName}")]
	public sealed class ObjectName : IEquatable<ObjectName>, IComparable<ObjectName>, ISqlFormattable {
		/// <summary>
		/// The special name used as a wild-card to indicate all the columns of a table
		/// must be referenced in a given context.
		/// </summary>
		public const char Glob = '*';

		/// <summary>
		/// The character that separates a name from its parent or child.
		/// </summary>
		public const char Separator = '.';

		private static readonly char[] InvalidNameChars = "\0.%^&({}+-/][\\".ToCharArray();

		/// <summary>
		/// Constructs a name reference without a parent.
		/// </summary>
		/// <param name="name">The object name.</param>
		/// <remarks>
		/// <b>NOTE:</b> This constructor is intended to be handling a name
		/// with no parent: if the string provided as <paramref name="name"/>
		/// contains any <see cref="Separator"/> character, this will make the
		/// resolution to fail at run-time. User <see cref="Parse"/> method to
		/// obtain a reference tree.
		/// </remarks>
		/// <exception cref="ArgumentNullException">
		/// If <paramref name="name"/> is <c>null</c> or empty.
		/// </exception>
		/// <seealso cref="ObjectName(ObjectName, String)"/>
		public ObjectName(string name) 
			: this(null, name) {
		}

		/// <summary>
		/// Constructs a name reference with a given parent.
		/// </summary>
		/// <param name="parent">The parent reference of the one being constructed</param>
		/// <param name="name">The name of the object.</param>
		/// <exception cref="ArgumentNullException">
		/// If <paramref name="name"/> is <c>null</c> or empty
		/// </exception>
		/// <exception cref="ArgumentException">If <paramref name="name"/> contains
		/// invalid characters (such as a separator).</exception>
		public ObjectName(ObjectName parent, string name) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException(nameof(name));

			if (!IsValidPart(name))
				throw new ArgumentException($"The name '{name}' contains invalid characters for a part of a complex name");
			
			Name = name;
			Parent = parent;
		}

		/// <summary>
		/// Gets the parent reference of the current one, if any or <c>null</c> if none.
		/// </summary>
		public ObjectName Parent { get; }

		public string ParentName => Parent?.FullName;

		/// <summary>
		/// Gets the name of the object being referenced.
		/// </summary>
		public string Name { get; }

		/// <summary>
		/// Gets the full reference name formatted.
		/// </summary>
		/// <seealso cref="ToString()"/>
		public string FullName {
			get { return ToString(); }
		}

		/// <summary>
		/// Indicates if this reference equivales to <see cref="Glob"/>.
		/// </summary>
		public bool IsGlob => Name.Length == 1 && Name[0].Equals(Glob);

		/// <summary>
		/// Parses the given string into a <see cref="ObjectName"/> object.
		/// </summary>
		/// <param name="s">The string to parse</param>
		/// <returns>
		/// Returns an instance of <see cref="ObjectName"/> that is the result
		/// of parsing of the string given.
		/// </returns>
		/// <exception cref="FormatException">
		/// If the given input string is of an invalid format.
		/// </exception>
		/// <exception cref="ArgumentNullException">
		/// If the given string is <c>null</c> or empty.
		/// </exception>
		public static ObjectName Parse(string s) {
			Exception error;
			ObjectName result;
			if (!TryParse(s, out result, out error))
				throw error;

			return result;
		}

		public static bool TryParse(string s, out ObjectName result) {
			Exception error;
			return TryParse(s, out result, out error);
		}

		private static bool TryParse(string s, out ObjectName result, out Exception error) {
			if (String.IsNullOrEmpty(s)) {
				error = new ArgumentNullException(nameof(s));
				result = null;
				return false;
			}

			var sp = s.Split(Separator);
			if (sp.Length == 0) {
				error = new FormatException("At least one part of the name must be provided");
				result = null;
				return false;
			}

			ObjectName finalName = null;
			for (int i = 0; i < sp.Length; i++) {
				var part = sp[i];
				if (String.IsNullOrEmpty(part)) {
					result = null;
					error = new FormatException("Cannot have one part of the name empty");
					return false;
				}

				if (!IsValidPart(part)) {
					result = null;
					error = new FormatException($"The name part '{part}' of the input '{s}' has invalid characters");
					return false;
				}

				if (finalName == null) {
					finalName = new ObjectName(part);
				} else {
					finalName = new ObjectName(finalName, part);
				}
			}

			result = finalName;
			error = null;
			return true;
		}

		/// <summary>
		/// Appends the given name to this one creating a new object name
		/// that is the result of the appending
		/// </summary>
		/// <param name="name">The name to append to this one</param>
		/// <returns>
		/// Returns an istance of <see cref="ObjectName"/> that has this
		/// one as parent, and is named by the given <paramref name="name"/>.
		/// </returns>
		public ObjectName Append(string name) {
			return new ObjectName(this, name);
		}

		public ObjectName Append(ObjectName name) {
			var baseName = this;
			ObjectName parent = name.Parent;
			while (parent != null) {
				baseName = baseName.Append(parent.Name);
				parent = parent.Parent;
			}

			baseName = baseName.Append(name.Name);
			return baseName;
		}

		/// <summary>
		/// Compares this instance of the object reference to a given one and
		/// returns a value indicating if the two instances equivales.
		/// </summary>
		/// <param name="other">The other object reference to compare.</param>
		/// <param name="ignoreCase">A boolean to indicate whether the comparison
		/// should be case-insensitive</param>
		/// <returns>
		/// Returns -1 if this instance preceeds the other name in the sort order,
		/// or 0 if the names are equivalent; returns 1 if this name follows the
		/// other name in the sort order.
		/// </returns>
		public int CompareTo(ObjectName other, bool ignoreCase) {
			if (other == null)
				return -1;

			int v = 0;
			if (Parent != null)
				v = Parent.CompareTo(other.Parent, ignoreCase);

			if (v == 0) {
				var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
				v = String.Compare(Name, other.Name, comparison);
			}

			return v;
		}

		/// <summary>
		/// Compares this instance of the object reference to a given one and
		/// returns a value indicating if the two instances equivales.
		/// </summary>
		/// <param name="other">The other object reference to compare.</param>
		/// <returns>
		/// Returns -1 if this instance preceeds the other name in the sort order,
		/// or 0 if the names are equivalent; returns 1 if this name follows the
		/// other name in the sort order.
		/// </returns>
		public int CompareTo(ObjectName other) {
			return CompareTo(other, false);
		}

		public override string ToString() {
			return this.ToSqlString();
		}


		public ObjectName ToUpper() {
			var upper = Name.ToUpperInvariant();
			ObjectName parent = null;
			if (Parent != null)
				parent = Parent.ToUpper();

			return new ObjectName(parent, upper);
		}

		public override bool Equals(object obj) {
			var other = obj as ObjectName;
			if (other == null)
				return false;

			return Equals(other);
		}

		/// <summary>
		/// Compares this object name with the other one given.
		/// </summary>
		/// <param name="other">The other <see cref="ObjectName"/> to compare.</param>
		/// <returns>
		/// Returns <c>true</c> if the two instances are equal, or <c>false</c> otherwise.
		/// </returns>
		public bool Equals(ObjectName other) {
			return Equals(other, false);
		}

		/// <summary>
		/// Compares this object name with the other one given, according to the
		/// case sensitivity specified.
		/// </summary>
		/// <param name="other">The other <see cref="ObjectName"/> to compare.</param>
		/// <param name="ignoreCase">The specification to either ignore the case for comparison.</param>
		/// <returns>
		/// Returns <c>true</c> if the two instances are equal, according to the case sensitivity
		/// given, or <c>false</c> otherwise.
		/// </returns>
		public bool Equals(ObjectName other, bool ignoreCase) {
			if (other == null)
				return false;

			if (Parent != null && other.Parent == null)
				return false;
			if (Parent == null && other.Parent != null)
				return false;

			if (Parent != null && !Parent.Equals(other.Parent, ignoreCase))
				return false;

			var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
			return String.Equals(Name, other.Name, comparison);
		}

		public override int GetHashCode() {
			return GetHashCode(false);
		}

		public int GetHashCode(bool ignoreCase) {
			var name = Name;
			if (ignoreCase)
				name = name.ToUpperInvariant();

			var code = name.GetHashCode() ^ 5623;
			if (Parent != null)
				code ^= Parent.GetHashCode(ignoreCase);

			return code;
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			if (Parent != null) {
				Parent.AppendTo(builder);
				builder.Append('.');
			}

			builder.Append(Name);
		}

		/// <summary>
		/// Verifies if the part of a name is valid
		/// </summary>
		/// <param name="name">The name part to validate</param>
		/// <returns>
		/// Returns <c>true</c> if the name part is valid,
		/// otherwise <c>false</c>.
		/// </returns>
		public static bool IsValidPart(string name) {
			return !String.IsNullOrWhiteSpace(name) &&
			       name.IndexOfAny(InvalidNameChars) < 0 &&
				   name.IndexOf(' ') < 0;
		}

		public static bool IsNullOrEmpty(ObjectName name) {
			if (name == null)
				return true;

			bool isNull = String.IsNullOrEmpty(name.Name);

			ObjectName parent = name.Parent;
			while (parent != null) {
				isNull |= IsNullOrEmpty(parent);
				parent = parent.Parent;
			}

			return isNull;
		}

	}
}