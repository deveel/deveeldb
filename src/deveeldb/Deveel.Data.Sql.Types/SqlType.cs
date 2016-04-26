// 
//  Copyright 2010-2016 Deveel
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
using System.IO;
using System.Linq;
using System.Runtime.Serialization;

using Deveel.Data.Sql.Compile;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Store;

namespace Deveel.Data.Sql.Types {
	/// <summary>
	/// Defines the properties of a specific SQL Type and handles the
	/// <see cref="ISqlObject">values compatible</see>.
	/// </summary>
	[Serializable]
	public abstract class SqlType : IComparer<ISqlObject>, IEquatable<SqlType>, ISerializable {
		/// <summary>
		/// Constructs the <see cref="SqlType"/> for the given specific
		/// <see cref="SqlTypeCode">SQL TYPE</see>.
		/// </summary>
		/// <remarks>
		/// This constructor will set the <see cref="Name"/> value to the equivalent
		/// of the SQL Type specified.
		/// </remarks>
		/// <param name="sqlType">The code of the SQL Type this object will represent.</param>
		protected SqlType(SqlTypeCode sqlType)
			: this(sqlType.ToString().ToUpperInvariant(), sqlType) {
		}

		/// <summary>
		/// Constructs the <see cref="SqlType"/> for the given specific
		/// <see cref="SqlTypeCode">SQL TYPE</see> and a given name.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="typeCode"></param>
		protected SqlType(string name, SqlTypeCode typeCode) {
			TypeCode = typeCode;
			Name = name;
		}

		protected SqlType(SerializationInfo info, StreamingContext context) {
			Name = info.GetString("Name");
			TypeCode = (SqlTypeCode) info.GetInt32("TypeCode");
		}

		/// <summary>
		/// Gets the name of the data-type that is used to resolve it within the context.
		/// </summary>
		/// <remarks>
		/// This value is always unique within a database system and can be simple
		/// (eg. for <see cref="IsPrimitive">primitive</see> types like <c>NUMERIC</c>),
		/// or composed by multiple parts (eg. for user-defined types).
		/// <para>
		/// Some primitive types (for example <c>NUMERIC</c>) can handle multiple SQL types,
		/// so this property works as an identificator for the type handler.
		/// </para>
		/// </remarks>
		public string Name { get; private set; }

		/// <summary>
		/// Gets the kind of SQL type this data-type handles.
		/// </summary>
		/// <remarks>
		/// The same instance of a <see cref="SqlType"/> can handle multiple
		/// kind of <see cref="SqlTypeCode">SQL types</see>, making such instances,
		/// of the same kind, to be different in attributes.
		/// <para>
		/// In fact, for example a <c>NUMERIC</c> data-type materialized as <c>INTEGER</c>
		/// is not equal to <c>NUMERIC</c> data-type materialized as <c>BIGINT</c>: the
		/// two instances will be comparable, but they won't be considered coincident.
		/// </para>
		/// </remarks>
		/// <see cref="IsComparable"/>
		public SqlTypeCode TypeCode { get; private set; }

		/// <summary>
		/// Indicates if the values handled by the type can be part of an index.
		/// </summary>
		/// <remarks>
		/// By default, this returns <c>true</c>, since most of primitive types
		/// are indexable (except for Long Objects).
		/// </remarks>
		public virtual bool IsIndexable {
			get { return true; }
		}

		/// <summary>
		/// Gets a value indicating if this data-type is primitive.
		/// </summary>
		public bool IsPrimitive {
			get { return IsPrimitiveType(TypeCode); }
		}

		public bool IsNull {
			get { return TypeCode == SqlTypeCode.Null; }
		}

		public virtual bool IsStorable {
			get { return false; }
		}

		/// <summary>
		/// Verifies if a given <see cref="SqlType"/> is comparable to
		/// this data-type.
		/// </summary>
		/// <param name="type">The other data-type to verify the compatibility.</param>
		/// <remarks>
		/// It is not required two <see cref="SqlType"/> to be identical to be compared:
		/// when overridden by a derived class, this methods verifies the properties of the
		/// argument type, to see if values handled by the types can be compared.
		/// <para>
		/// By default, this method compares the values returned by <see cref="TypeCode"/>
		/// to see if they are identical.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns <c>true</c> if the values handled by this data-type can be compared to ones handled 
		/// by the given <paramref name="type"/>, or <c>false</c> otherwise.
		/// </returns>
		public virtual bool IsComparable(SqlType type) {
			return TypeCode == type.TypeCode;
		}

		/// <summary>
		/// Verifies if this type can cast any value to the given <see cref="SqlType"/>.
		/// </summary>
		/// <param name="destType">The other type, destination of the cast, to verify.</param>
		/// <remarks>
		/// By default, this method returns <c>false</c>, because cast process must be
		/// specified per type: when overriding the method <see cref="CastTo"/>, pay attention
		/// to also override this method accordingly.
		/// </remarks>
		/// <returns>
		/// </returns>
		/// <see cref="CastTo"/>
		public virtual bool CanCastTo(SqlType destType) {
			return false;
		}

		/// <summary>
		/// Converts the given <see cref="Field">object value</see> to a
		/// <see cref="SqlType"/> specified.
		/// </summary>
		/// <param name="value">The value to convert.</param>
		/// <param name="destType">The destination type of the conversion.</param>
		/// <remarks>
		/// If the given <paramref name="destType">destination type</paramref> is equivalent
		/// to this type, it will return the <paramref name="value"/> provided, otherwise
		/// it will throw an exception by default.
		/// <para>
		/// Casting values to specific types is specific to each data-type: override this
		/// method to support type-specific conversions.
		/// </para>
		/// <para>
		/// When overriding this method, <see cref="CanCastTo"/> should be overridden accordingly
		/// to indicate the type supports casting.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="Field"/> that is the result
		/// of the conversion from this data-type to the other type given.
		/// </returns>
		public virtual ISqlObject CastTo(ISqlObject value, SqlType destType) {
			if (Equals(destType))
				return value;

			// TODO: Should we return a null value instead? NULL OF TYPE anyway is still a cast ...
			throw new NotSupportedException();
		}

		public virtual object ConvertTo(ISqlObject obj, Type destType) {
			throw new NotSupportedException();
		}

		public virtual ISqlObject Add(ISqlObject a, ISqlObject b) {
			return SqlNull.Value;
		}

		public virtual ISqlObject Subtract(ISqlObject a, ISqlObject b) {
			return SqlNull.Value;
		}

		public virtual ISqlObject Multiply(ISqlObject a, ISqlObject b) {
			return SqlNull.Value;
		}

		public virtual ISqlObject Divide(ISqlObject a, ISqlObject b) {
			return SqlNull.Value;
		}

		public virtual ISqlObject Modulus(ISqlObject a, ISqlObject b) {
			return SqlNull.Value;
		}

		public virtual ISqlObject Negate(ISqlObject value) {
			return SqlNull.Value;
		}

		public virtual SqlBoolean IsEqualTo(ISqlObject a, ISqlObject b) {
			if (!a.IsComparableTo(b))
				return SqlBoolean.Null;

			return a.CompareTo(b) == 0;
		}

		public virtual SqlBoolean IsNotEqualTo(ISqlObject a, ISqlObject b) {
			if (!a.IsComparableTo(b))
				return SqlBoolean.Null;

			return a.CompareTo(b) != 0;
		}

		public virtual SqlBoolean IsGreatherThan(ISqlObject a, ISqlObject b) {
			if (!a.IsComparableTo(b))
				return SqlBoolean.Null;

			return a.CompareTo(b) > 0;
		}

		public virtual SqlBoolean IsSmallerThan(ISqlObject a, ISqlObject b) {
			if (!a.IsComparableTo(b))
				return SqlBoolean.Null;

			return a.CompareTo(b) < 0;
		}

		public virtual SqlBoolean IsGreaterOrEqualThan(ISqlObject a, ISqlObject b) {
			if (!a.IsComparableTo(b))
				return SqlBoolean.Null;

			return a.CompareTo(b) >= 0;
		}

		public virtual SqlBoolean IsSmallerOrEqualThan(ISqlObject a, ISqlObject b) {
			if (!a.IsComparableTo(b))
				return SqlBoolean.Null;

			return a.CompareTo(b) <= 0;
		}

		public virtual ISqlObject And(ISqlObject a, ISqlObject b) {
			return SqlBoolean.Null;
		}

		public virtual ISqlObject Or(ISqlObject a, ISqlObject b) {
			return SqlBoolean.Null;
		}

		public virtual ISqlObject XOr(ISqlObject x, ISqlObject y) {
			return SqlNull.Value;
		}

		public virtual ISqlObject UnaryPlus(ISqlObject value) {
			return SqlNull.Value;
		}

		public virtual ISqlObject Reverse(ISqlObject value) {
			return SqlNull.Value;
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Name", Name);
			info.AddValue("TypeCode", (int) TypeCode);

			GetData(info, context);
		}

		protected virtual void GetData(SerializationInfo info, StreamingContext context) {
		}

		/// <summary>
		/// Gets the one data-type between this and the other one given
		/// that handles the wider range of values.
		/// </summary>
		/// <param name="otherType">The other type to verify.</param>
		/// <remarks>
		/// This is very important for operations and functions, when
		/// operating on <see cref="Field">objects</see> with comparable
		/// but different data-types, to ensure the result of the operation
		/// will be capable to handle the final value.
		/// <para>
		/// By default, this method returns this instance, as it is not able
		/// to dynamically define the wider type.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns this type if it handles a wider range of values or <paramref name="otherType">other 
		/// type</paramref> given otherwise.
		/// </returns>
		public virtual SqlType Wider(SqlType otherType) {
			return this;
		}

		/// <summary>
		/// Parses a SQL formatted string that defines a data-type into
		/// a constructed <see cref="SqlType"/> object equivalent.
		/// </summary>
		/// <param name="s">The SQL formatted data-type string, defining the properties of the type.</param>
		/// <remarks>
		/// This method only supports primitive types.
		/// </remarks>
		/// <returns>
		/// </returns>
		/// <seealso cref="PrimitiveTypes.IsPrimitive(SqlTypeCode)"/>
		/// <seealso cref="ToString()"/>
		public static SqlType Parse(string s) {
			return Parse(null, s);
		}

		/// <summary>
		/// Parses a SQL formatted string that defines a data-type into
		/// a constructed <see cref="SqlType"/> object equivalent.
		/// </summary>
		/// <param name="context">A context used to resolve the SQL parser.</param>
		/// <param name="s">The SQL formatted data-type string, defining the properties of the type.</param>
		/// <remarks>
		/// If the <paramref name="context"/> is not provided, this will fail in case of
		/// non-primitive types.
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="SqlType"/> that corresponds to the
		/// string provided.
		/// </returns>
		/// <exception cref="FormatException">
		/// If the provided string does not resolve to any valid <see cref="SqlType"/>
		/// </exception>
		/// <exception cref="FormatException">
		/// If the string does not resolve to any primitive types and the <paramref name="context"/>
		/// is <c>null</c> or the type is not found in the context.
		/// </exception>
		/// <seealso cref="PrimitiveTypes.IsPrimitive(SqlTypeCode)"/>
		/// <seealso cref="ToString()"/>
		public static SqlType Parse(IContext context, string s) {
			try {
				IDataTypeParser parser = null;
				if (context != null)
					parser = context.ResolveService<IDataTypeParser>();

				if (parser == null)
					parser = new DefaultDataTypeParser();

				var info = parser.Parse(s);

				if (info == null)
					throw new InvalidOperationException("Invalid response from parser.");

				if (info.IsPrimitive)
					return PrimitiveTypes.Resolve(info.TypeName, info.Metadata);

				if (context == null)
					throw new NotSupportedException(String.Format("The type '{0}' is not primitive and no resolve context is provided.", info.TypeName));

				return context.TypeResolver().ResolveType(new TypeResolveContext(SqlTypeCode.Unknown, info.TypeName, info.Metadata));
			} catch (Exception ex) {
				throw new FormatException(String.Format("Unable to parse the string '{0}' to a valid data type.", s), ex);
			}
		}

		#region DefaultDataTypeParser

		class DefaultDataTypeParser : IDataTypeParser {
			public DataTypeInfo Parse(string s) {
				return new PlSqlCompiler().ParseDataType(s);
			}
		}

		#endregion

		/// <inheritdoc/>
		public virtual int Compare(ISqlObject x, ISqlObject y) {
			if (!x.IsComparableTo(y))
				throw new NotSupportedException();

			if (x.IsNull && y.IsNull)
				return 0;
			if (x.IsNull && !y.IsNull)
				return 1;
			if (!x.IsNull && y.IsNull)
				return -1;

			return ((IComparable) x).CompareTo(y);
		}

		/// <inheritdoc/>
		public override bool Equals(object obj) {
			var dataType = obj as SqlType;
			if (dataType == null)
				return false;

			return Equals(dataType);
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return TypeCode.GetHashCode();
		}

		/// <inheritdoc/>
		public virtual bool Equals(SqlType other) {
			if (other == null)
				return false;

			return TypeCode == other.TypeCode;
		}

		/// <inheritdoc/>
		public override string ToString() {
			return TypeCode.ToString().ToUpperInvariant();
		}

		public virtual void SerializeObject(Stream stream, ISqlObject obj) {
			throw new NotSupportedException(String.Format("Type {0} cannot serialize object of type {1}.", GetType(),
				obj.GetType()));
		}

		public virtual ISqlObject DeserializeObject(Stream stream) {
			throw new NotSupportedException(String.Format("Type {0} cannot deserialize types.", GetType()));
		}

		public virtual bool IsCacheable(ISqlObject value) {
			return false;
		}

		internal virtual int GetCacheUsage(ISqlObject value) {
			return 0;
		}

		internal virtual int ColumnSizeOf(ISqlObject obj) {
			// TODO: should make this required?
			return 0;
		}

		public virtual Type GetRuntimeType() {
			throw new NotSupportedException();
		}

		public virtual Type GetObjectType() {
			throw new NotSupportedException();
		}

		public virtual ISqlObject CreateFromLargeObject(ILargeObject objRef) {
			throw new NotSupportedException(String.Format("SQL Type {0} cannot be created from a large object.", TypeCode));
		}

		public static bool IsPrimitiveType(SqlTypeCode typeCode) {
			return PrimitiveTypes.IsPrimitive(typeCode);
		}

		public virtual ISqlObject CreateFrom(object value) {
			throw new NotSupportedException(String.Format("The type {0} does not support runtime object conversion.", ToString()));
		}

		public static SqlType Resolve(SqlTypeCode typeCode, DataTypeMeta[] meta) {
			return Resolve(typeCode, meta, null);
		}

		public static SqlType Resolve(SqlTypeCode typeCode, DataTypeMeta[] meta, ITypeResolver resolver) {
			return Resolve(typeCode, typeCode.ToString().ToUpperInvariant(), meta, resolver);
		}

		public static SqlType Resolve(SqlTypeCode typeCode, string name, DataTypeMeta[] meta, ITypeResolver resolver) {
			return TypeResolver.Resolve(typeCode, name, meta, resolver);
		}

		public static SqlTypeCode GetTypeCode(Type type) {
			if (type == null)
				return SqlTypeCode.Unknown;

			if (type == typeof(bool))
				return SqlTypeCode.Boolean;
			if (type == typeof(byte))
				return SqlTypeCode.TinyInt;
			if (type == typeof(short))
				return SqlTypeCode.SmallInt;
			if (type == typeof(int))
				return SqlTypeCode.Integer;
			if (type == typeof(long))
				return SqlTypeCode.BigInt;
			if (type == typeof(float))
				return SqlTypeCode.Real;
			if (type == typeof(double))
				return SqlTypeCode.Double;
			if (type == typeof(DateTime) ||
				type == typeof(DateTimeOffset))
				return SqlTypeCode.TimeStamp;
			if (type == typeof(string))
				return SqlTypeCode.String;
			if (type == typeof(byte[]))
				return SqlTypeCode.Binary;

			if (type == typeof(SqlBoolean))
				return SqlTypeCode.Boolean;
			if (type == typeof(SqlNumber))
				return SqlTypeCode.Numeric;
			if (type == typeof(SqlDateTime))
				return SqlTypeCode.TimeStamp;
			if (type == typeof(SqlString))
				return SqlTypeCode.String;

			throw new NotSupportedException(String.Format("The type '{0}' is not supported.", type));
		}
	}
}