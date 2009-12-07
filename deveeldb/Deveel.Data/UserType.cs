//  
//  UserType.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

using Deveel.Data.Text;

namespace Deveel.Data {
	/// <summary>
	/// Describes the metadata of a user-defined type.
	/// </summary>
	public sealed class UserType {
		/// <summary>
		/// Constructs a <see cref="UserType"/> having the
		/// given name and deriving from a nother type.
		/// </summary>
		/// <param name="name">The name of the type defined.</param>
		/// <param name="parent">The name of the parent type.</param>
		/// <param name="attributes"></param>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="name"/> of the type is
		/// <c>null</c>.
		/// </exception>
		public UserType(UserType parent, TableName name, UserTypeAttributes attributes) {
			if (name == null)
				throw new ArgumentNullException("name");

			if ((attributes & UserTypeAttributes.Abstract) != 0 &&
				((attributes & UserTypeAttributes.Sealed) != 0 ||
				 (attributes & UserTypeAttributes.Primitive) != 0))
				throw new ArgumentException("The type attributes specified are invalid: a primitive or sealed " +
				                            "type cannot be abstract");

			this.attributes = attributes;
			this.name = name;
			this.parent = parent;
			members = new ArrayList();
		}

		/// <summary>
		/// Constructs a <see cref="UserType"/> having the
		/// given name.
		/// </summary>
		/// <param name="name">The name of the type defined.</param>
		/// <param name="attributes">The flags defining the attributes
		/// of the UDT.</param>
		public UserType(TableName name, UserTypeAttributes attributes)
			: this(null, name, attributes) {
		}

		/// <summary>
		/// The unique name of the type.
		/// </summary>
		private readonly TableName name;

		/// <summary>
		/// The parent type of this type.
		/// </summary>
		private readonly UserType parent;

		/// <summary>
		/// The UDT attributes.
		/// </summary>
		private readonly UserTypeAttributes attributes;

		/// <summary>
		/// A list containing all the members of the type.
		/// </summary>
		private readonly ArrayList members;

		/// <summary>
		/// In case the UDT is derived from an external type, this
		/// is a reference to it.
		/// </summary>
		private Type externalType;

		/// <summary>
		/// The string referencing the external type within the
		/// mapping context.
		/// </summary>
		private string externalTypeName;

		private bool immutable;

		public UserType ParentType {
			get { return parent; }
		}

		public TableName Name {
			get { return name; }
		}

		public bool IsFromPrimitive {
			get { return parent != null && parent.IsPrimitive; }
		}

		public bool IsPrimitive {
			get { return (attributes & UserTypeAttributes.Primitive) != 0; }
		}

		public bool IsSealed {
			get { return (attributes & UserTypeAttributes.Sealed) != 0; }
		}

		public bool IsAbstract {
			get { return (attributes & UserTypeAttributes.Abstract) != 0; }
		}

		public UserTypeAttributes Attributes {
			get { return attributes; }
		}

		public bool IsExternal {
			get { return externalType != null; }
		}

		public int MemberCount {
			get { return members.Count; }
		}

		public bool IsReadOnly {
			get { return immutable; }
		}

		public TType TType {
			get {
				if (!IsFromPrimitive)
					throw new InvalidOperationException("The type is not derived from a primitive.");

				return GetAttribute(0).Type;
			}
		}

		internal string ExternalTypeString {
			get { return externalTypeName; }
		}

		public UserTypeAttribute GetAttribute(int index) {
			return members[index] as UserTypeAttribute;
		}

		public UserTypeAttribute FindAttribute(string name) {
			for (int i = 0; i < members.Count; i++) {
				UserTypeAttribute attribute = (UserTypeAttribute) members[i];
				if (attribute.Name == name)
					return attribute;
			}

			return null;
		}

		public UserTypeAttribute AddMember(string name, TType type, bool nullable) {
			if (immutable)
				throw new InvalidOperationException("This element is not modifiable.");

			if (IsPrimitive)
				throw new InvalidOperationException("Cannot add an attribute to a primitive type.");
			if (IsFromPrimitive)
				throw new InvalidOperationException("Cannot add an attribute to a type derived from a primitive.");

			if (FindAttribute(name) != null)
				throw new ArgumentException("The member '" + name + "' is already defined in type '" + this.name + "'.");

			UserTypeAttribute attribute = new UserTypeAttribute(this, name, type);
			attribute.SetOffset(members.Count);
			members.Add(attribute);
			return attribute;
		}

		public UserTypeAttribute AddMember(string name, TType type) {
			return AddMember(name, type, false);
		}

		internal void SetReadOnly() {
			immutable = true;
		}

		public static UserType FromPrimitive(DbType dbType, TableName name, int size, int scale) {
			UserType parent;
			TType ttype;
			switch(dbType) {
				case DbType.Boolean:
					parent = BooleanType;
					ttype = TType.GetBooleanType(SqlType.Boolean);
					break;
				case DbType.Numeric:
					parent = NumericType;
					ttype = TType.GetNumericType(SqlType.Numeric, size, scale);
					break;
				case DbType.String:
					parent = StringType;
					ttype = TType.GetStringType(SqlType.VarChar, size, null, CollationStrength.None, CollationDecomposition.None);
					break;
				case DbType.Time:
					parent = TimeType;
					ttype = TType.GetDateType(SqlType.TimeStamp);
					break;
				case DbType.Binary:
					parent = BinaryType;
					ttype = TType.GetBinaryType(SqlType.Binary, size);
					break;
				default:
					throw new ArgumentException("Cannot derive from the given primitive type.");
			}

			UserType userType = new UserType(parent, name, UserTypeAttributes.Sealed);
			userType.members.Add(new UserTypeAttribute(userType, SingleAttributeName, ttype));
			return userType;
		}

		public static UserType FromPrimitive(DbType dbType, TableName name, int size) {
			return FromPrimitive(dbType, name, size, -1);
		}

		public static UserType FromPrimitive(DbType dbType, TableName name) {
			return FromPrimitive(dbType, name, -1);
		}

		public static UserType FromType(Type type, TableName name) {
			UserType userType = MappingContext.CreateUserType(type, name);
			userType.externalType = type;
			return userType;
		}

		public static UserType FromType(string typeString, TableName name) {
			Type type = MappingContext.GetType(typeString);
			UserType userType = FromType(type, name);
			userType.externalTypeName = typeString;
			return userType;
		}

		internal const string SingleAttributeName = "##TYPE_ATTRIBUTE##";

		internal static readonly TableName BooleanTypeName = new TableName("BOOLEAN");
		internal static readonly TableName NumericTypeName = new TableName("NUMERIC");
		internal static readonly TableName StringTypeName = new TableName("STRING");
		internal static readonly TableName BinaryTypeName = new TableName("BINARY");
		internal static readonly TableName TimeTypeName = new TableName("TIME");
		internal static readonly TableName IntervalTypeName = new TableName("INTERVAL");

		private static readonly UserType BooleanType = new UserType(BooleanTypeName, UserTypeAttributes.Primitive);
		private static readonly UserType NumericType = new UserType(NumericTypeName, UserTypeAttributes.Primitive);
		private static readonly UserType StringType = new UserType(StringTypeName, UserTypeAttributes.Primitive);
		private static readonly UserType TimeType = new UserType(TimeTypeName, UserTypeAttributes.Primitive);
		private static readonly UserType BinaryType = new UserType(BinaryTypeName, UserTypeAttributes.Primitive);
	}
}