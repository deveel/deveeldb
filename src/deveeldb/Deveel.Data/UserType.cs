// 
//  Copyright 2010  Deveel
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

		/// <summary>
		/// Gets the reference to the parent <see cref="UserType"/>,
		/// if this inherits from any type.
		/// </summary>
		public UserType ParentType {
			get { return parent; }
		}

		/// <summary>
		/// Gets the fully qualified name of the type.
		/// </summary>
		public TableName Name {
			get { return name; }
		}

		/// <summary>
		/// Gets a boolean value indicating if this type was
		/// derived from a primitive type.
		/// </summary>
		/// <remarks>
		/// Types derived from primitives cannot be inherited
		/// and cannot contain any member.
		/// </remarks>
		public bool IsFromPrimitive {
			get { return parent != null && parent.IsPrimitive; }
		}

		/// <summary>
		/// Gets a boolean value indicating if this type is a
		/// primitive type.
		/// </summary>
		/// <remarks>
		/// In the context of user-defined types, this has a special
		/// value, since primitive user-types cannot be constructed
		/// outside the system and are used as referenced for special
		/// inheritance.
		/// </remarks>
		public bool IsPrimitive {
			get { return (attributes & UserTypeAttributes.Primitive) != 0; }
		}

		/// <summary>
		/// Gets a value indicating whether this type can be
		/// inherited by other types.
		/// </summary>
		public bool IsSealed {
			get { return (attributes & UserTypeAttributes.Sealed) != 0; }
		}

		/// <summary>
		/// Gets a value indicating whether this type must be
		/// inherited by other types.
		/// </summary>
		public bool IsAbstract {
			get { return (attributes & UserTypeAttributes.Abstract) != 0; }
		}

		/// <summary>
		/// Gets the <see cref="UserTypeAttributes">type attributes</see>
		/// of this user-defined type.
		/// </summary>
		public UserTypeAttributes Attributes {
			get { return attributes; }
		}

		/// <summary>
		/// Gets a boolean value whether this type was obtained
		/// by reflecting an external <see cref="System.Type"/>.
		/// </summary>
		public bool IsExternal {
			get { return externalType != null; }
		}

		/// <summary>
		/// Gets the number of members defined by this type.
		/// </summary>
		public int MemberCount {
			get { return members.Count; }
		}

		internal bool IsReadOnly {
			get { return immutable; }
		}

		/// <summary>
		/// If this type is derived <see cref="IsFromPrimitive">from a primitive</see>
		/// type, gets the primitive <see cref="TType"/> of this type.
		/// </summary>
		/// <exception cref="InvalidOperationException">
		/// If the type is not <see cref="IsFromPrimitive"/>.
		/// </exception>
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

		/// <summary>
		/// Gets the type attribute at the given index.
		/// </summary>
		/// <param name="index">The index at which to get the type
		/// attribute.</param>
		/// <returns>
		/// Returns a <see cref="UserTypeAttribute"/> at the given
		/// index.
		/// </returns>
		public UserTypeAttribute GetAttribute(int index) {
			return members[index] as UserTypeAttribute;
		}

		/// <summary>
		/// Tries to get an attribute having the given name.
		/// </summary>
		/// <param name="attrName">The name of the attribute to find.</param>
		/// <returns>
		/// Returns a <see cref="UserTypeAttribute">type attribute</see>
		/// </returns>
		public UserTypeAttribute FindAttribute(string attrName) {
			for (int i = 0; i < members.Count; i++) {
				UserTypeAttribute attribute = (UserTypeAttribute) members[i];
				if (attribute.Name == attrName)
					return attribute;
			}

			return null;
		}

		/// <summary>
		/// Adds a new <see cref="UserTypeAttribute"/> to the
		/// type, having the given name and type.
		/// </summary>
		/// <param name="attrName">The name of the attribute.</param>
		/// <param name="type">The type of the attribute.</param>
		/// <param name="nullable">Whether the attribute can accept <c>null</c>
		/// values or not.</param>
		/// <returns>
		/// Returns a reference to the <see cref="UserTypeAttribute">attribute</see>
		/// just created within the type.
		/// </returns>
		public UserTypeAttribute AddAttribute(string attrName, TType type, bool nullable) {
			if (immutable)
				throw new InvalidOperationException("This element is not modifiable.");

			if (IsPrimitive)
				throw new InvalidOperationException("Cannot add an attribute to a primitive type.");
			if (IsFromPrimitive)
				throw new InvalidOperationException("Cannot add an attribute to a type derived from a primitive.");

			if (FindAttribute(attrName) != null)
				throw new ArgumentException("The member '" + attrName + "' is already defined in type '" + name + "'.");

			UserTypeAttribute attribute = new UserTypeAttribute(this, attrName, type);
			attribute.SetOffset(members.Count);
			members.Add(attribute);
			return attribute;
		}

		/// <summary>
		/// Adds a new <see cref="UserTypeAttribute"/> to the
		/// type, having the given name and type.
		/// </summary>
		/// <param name="attrName">The name of the attribute.</param>
		/// <param name="type">The type of the attribute.</param>
		/// <returns>
		/// Returns a reference to the <see cref="UserTypeAttribute">attribute</see>
		/// just created within the type.
		/// </returns>
		public UserTypeAttribute AddAttribute(string attrName, TType type) {
			return AddAttribute(attrName, type, false);
		}

		internal void SetReadOnly() {
			immutable = true;
		}

		/// <summary>
		/// Constructs a <see cref="UserType"/> from a given primitive type,
		/// having the specified name and size.
		/// </summary>
		/// <param name="sqlType">The database primitive type to inherit from.</param>
		/// <param name="name">The name of the type to build.</param>
		/// <param name="size">The optional size to set to the type.</param>
		/// <param name="scale">The optional numeric scale to set to the type.</param>
		/// <returns>
		/// Returns an instance of <see cref="UserType"/> that is built on
		/// a given <see cref="DbType">database primitive</see>.
		/// </returns>
		public static UserType FromPrimitive(SqlType sqlType, TableName name, int size, int scale) {
			UserType parent;
			TType ttype;
			switch(sqlType) {
				case SqlType.Bit:
				case SqlType.Boolean:
					parent = BooleanType;
					ttype = TType.GetBooleanType(sqlType);
					break;
				case SqlType.Numeric:
				case SqlType.TinyInt:
				case SqlType.SmallInt:
				case SqlType.Integer:
				case SqlType.BigInt:
				case SqlType.Float:
				case SqlType.Double:
				case SqlType.Real:
					parent = NumericType;
					ttype = TType.GetNumericType(sqlType, size, scale);
					break;
				case SqlType.Char:
				case SqlType.VarChar:
					parent = StringType;
					ttype = TType.GetStringType(sqlType, size, null, CollationStrength.None, CollationDecomposition.None);
					break;
				case SqlType.Date:
				case SqlType.Time:
				case SqlType.TimeStamp:
					parent = TimeType;
					ttype = TType.GetDateType(sqlType);
					break;
				case SqlType.Interval:
					parent = IntervalType;
					ttype = TType.GetIntervalType(sqlType);
					break;
				case SqlType.Binary:
				case SqlType.VarBinary:
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

		/// <summary>
		/// Constructs a <see cref="UserType"/> from a given primitive type,
		/// having the specified name and size.
		/// </summary>
		/// <param name="dbType">The database primitive type to inherit from.</param>
		/// <param name="name">The name of the type to build.</param>
		/// <param name="size">The optional size to set to the type.</param>
		/// <returns>
		/// Returns an instance of <see cref="UserType"/> that is built on
		/// a given <see cref="DbType">database primitive</see>.
		/// </returns>
		public static UserType FromPrimitive(SqlType dbType, TableName name, int size) {
			return FromPrimitive(dbType, name, size, -1);
		}

		/// <summary>
		/// Constructs a <see cref="UserType"/> from a given primitive type,
		/// having the specified name.
		/// </summary>
		/// <param name="dbType">The database primitive type to inherit from.</param>
		/// <param name="name">The name of the type to build.</param>
		/// <returns>
		/// Returns an instance of <see cref="UserType"/> that is built on
		/// a given <see cref="DbType">database primitive</see>.
		/// </returns>
		public static UserType FromPrimitive(SqlType dbType, TableName name) {
			return FromPrimitive(dbType, name, -1);
		}

		/// <summary>
		/// Constructs a <see cref="UserType"/> from an external
		/// <see cref="System.Type"/> given, having the specified
		/// name.
		/// </summary>
		/// <param name="type">The external <see cref="System.Type"/> used to
		/// model the <see cref="UserType"/> to create.</param>
		/// <param name="name">The fully qualified name of the type to build.</param>
		/// <returns>
		/// Returns an instance of <see cref="UserType"/> modeled on the
		/// given external <see cref="System.Type"/>.
		/// </returns>
		public static UserType FromType(Type type, TableName name) {
			UserType userType = MappingContext.CreateUserType(type, name);
			userType.externalType = type;
			return userType;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="typeString"></param>
		/// <param name="name"></param>
		/// <returns></returns>
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
		private static readonly UserType IntervalType = new UserType(IntervalTypeName, UserTypeAttributes.Primitive);
		private static readonly UserType BinaryType = new UserType(BinaryTypeName, UserTypeAttributes.Primitive);
	}
}