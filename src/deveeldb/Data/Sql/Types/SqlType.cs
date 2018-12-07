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
using System.Collections;
using System.Collections.Generic;
using System.Runtime.Serialization;

using Deveel.Data.Services;
using Deveel.Data.Sql.Parsing;

namespace Deveel.Data.Sql.Types {
	/// <summary>
	/// Defines the properties of a specific SQL Type and handles the
	/// <see cref="ISqlValue">values compatible</see>.
	/// </summary>
	/// <remarks>
	/// SQL Types provides the properties of values in a column of a
	/// database table, handling conversions, arithmetic operations,
	/// runtime transposition, serialization of values, and other
	/// operations.
	/// </remarks>
	[Serializable]
	public abstract class SqlType : IEquatable<SqlType>, IComparer, IComparer<ISqlValue>, ISqlFormattable, ISerializable {
		/// <summary>
		/// Constructs the <see cref="SqlType"/> for the given specific
		/// <see cref="SqlTypeCode">SQL TYPE</see>.
		/// </summary>
		/// <param name="typeCode">The code of the SQL Type this object will represent.</param>
		protected SqlType(SqlTypeCode typeCode) {
			TypeCode = typeCode;
		}

		protected SqlType(SerializationInfo info, StreamingContext context) {
			TypeCode = (SqlTypeCode) info.GetInt32("typeCode");
		}

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
		public SqlTypeCode TypeCode { get; }

		/// <summary>
		/// Indicates if the values handled by the type can be part of an index.
		/// </summary>
		/// <remarks>
		/// By default, this returns <c>true</c>, if this is a primitive type
		/// and not a large object.
		/// </remarks>
		public virtual bool IsIndexable => IsPrimitive && !IsLargeObject;

		/// <summary>
		/// Gets a value indicating whether this type handles large objects.
		/// </summary>
		/// <value>
		/// <c>true</c> if this instance handles large objects; otherwise <c>false</c>.
		/// </value>
		public bool IsLargeObject => TypeCode == SqlTypeCode.Clob ||
		                             TypeCode == SqlTypeCode.Blob ||
		                             TypeCode == SqlTypeCode.LongVarChar ||
		                             TypeCode == SqlTypeCode.LongVarBinary;

		/// <summary>
		/// Gets a value indicating if this data-type is primitive.
		/// </summary>
		public bool IsPrimitive => IsPrimitiveType(TypeCode);

		/// <summary>
		/// Gets a value indicating whether this instance is reference to another type.
		/// </summary>
		/// <value>
		/// <c>true</c> if this instance is reference to another type; otherwise, <c>false</c>.
		/// </value>
		public virtual bool IsReference => false;

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

		#region Equals

		/// <inheritdoc/>
		public override bool Equals(object obj) {
			var dataType = obj as SqlType;
			if (dataType == null)
				return false;

			return Equals(dataType);
		}

		/// <inheritdoc/>
		public virtual bool Equals(SqlType other) {
			if (other == null)
				return false;

			return TypeCode == other.TypeCode;
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return TypeCode.GetHashCode();
		}

		#endregion

		public virtual bool IsInstanceOf(ISqlValue value) {
			return value is SqlNull;
		}


		#region ISqlFormattable

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			AppendTo(builder);
		}

		protected virtual void AppendTo(SqlStringBuilder sqlBuilder) {
			sqlBuilder.Append(TypeCode.ToString().ToUpperInvariant());
		}

		public override string ToString() {
			return this.ToSqlString();
		}

		public virtual string ToSqlString(ISqlValue value) {
			return value.ToString();
		}

		#endregion

		int IComparer.Compare(object x, object y) {
			if (!(x is ISqlValue))
				throw new ArgumentException("The argument is not a SQL Value", nameof(x));
			if (!(y is ISqlValue))
				throw new ArgumentException("The argument is not a SQL Value", nameof(y));

			return (this as IComparer<ISqlValue>).Compare((ISqlValue) x, (ISqlValue) y);
		}

		public virtual int Compare(ISqlValue x, ISqlValue y) {
			if (x == null && y == null)
				return 0;
			if (x == null)
				return 1;
			if (y == null)
				return -1;

			if (!x.IsComparableTo(y))
				throw new NotSupportedException();

			return ((IComparable) x).CompareTo(y);
		}

		/// <summary>
		/// Gets the one data-type between this and the other one given
		/// that handles the wider range of values.
		/// </summary>
		/// <param name="otherType">The other type to verify.</param>
		/// <remarks>
		/// This is very important for operations and functions, when
		/// operating on <see cref="SqlObject">objects</see> with comparable
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

		public static bool IsPrimitiveType(SqlTypeCode typeCode) {
			return PrimitiveTypes.IsPrimitive(typeCode);
		}

		#region Cast

		/// <summary>
		/// Verifies if this type can cast any value to the given <see cref="SqlType"/>.
		/// </summary>
		/// <param name="value">The value to be cast</param>
		/// <param name="destType">The other type, destination of the cast, to verify.</param>
		/// <remarks>
		/// By default, this method returns <c>false</c>, because cast process must be
		/// specified per type: when overriding the method <see cref="Cast"/>, pay attention
		/// to also override this method accordingly.
		/// </remarks>
		/// <returns>
		/// </returns>
		/// <see cref="Cast"/>
		public virtual bool CanCastTo(ISqlValue value, SqlType destType) {
			return false;
		}

		/// <summary>
		/// Converts the given <see cref="ISqlValue">object value</see> to a
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
		/// Returns an instance of <see cref="ISqlValue"/> that is the result
		/// of the conversion from this data-type to the other type given.
		/// </returns>
		public virtual ISqlValue Cast(ISqlValue value, SqlType destType) {
			if (Equals(destType))
				return value;

			return SqlNull.Value;
		}

		public virtual ISqlValue NormalizeValue(ISqlValue value) {
			return value;
		}

		#endregion

		#region Serialization

		protected virtual void GetObjectData(SerializationInfo info) {

		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("typeCode", (int)TypeCode);
			GetObjectData(info);
		}

		#endregion

		#region Operators

		#region Binary Operators

		private void AssertComparable(ISqlValue a, ISqlValue b) {
			if (a == null || b == null)
				return;

			if (!a.IsComparableTo(b))
				throw new ArgumentException("Values are not comparable");
		}

		public virtual ISqlValue Add(ISqlValue a, ISqlValue b) {
			return SqlNull.Value;
		}

		public virtual ISqlValue Subtract(ISqlValue a, ISqlValue b) {
			return SqlNull.Value;
		}

		public virtual ISqlValue Multiply(ISqlValue a, ISqlValue b) {
			return SqlNull.Value;
		}

		public virtual ISqlValue Divide(ISqlValue a, ISqlValue b) {
			return SqlNull.Value;
		}

		public virtual ISqlValue Modulo(ISqlValue a, ISqlValue b) {
			return SqlNull.Value;
		}

		public virtual SqlBoolean Equal(ISqlValue a, ISqlValue b) {
			AssertComparable(a, b);
			return a.CompareTo(b) == 0;
		}

		public virtual SqlBoolean NotEqual(ISqlValue a, ISqlValue b) {
			AssertComparable(a, b);
			return a.CompareTo(b) != 0;
		}

		public virtual SqlBoolean Greater(ISqlValue a, ISqlValue b) {
			AssertComparable(a, b);
			return a.CompareTo(b) < 0;
		}

		public virtual SqlBoolean Less(ISqlValue a, ISqlValue b) {
			AssertComparable(a, b);
			return a.CompareTo(b) > 0;
		}

		public virtual SqlBoolean GreaterOrEqual(ISqlValue a, ISqlValue b) {
			AssertComparable(a, b);
			return a.CompareTo(b) <= 0;
		}

		public virtual SqlBoolean LessOrEqual(ISqlValue a, ISqlValue b) {
			AssertComparable(a, b);
			return a.CompareTo(b) >= 0;
		}

		public virtual ISqlValue And(ISqlValue a, ISqlValue b) {
			return SqlNull.Value;
		}

		public virtual ISqlValue Or(ISqlValue a, ISqlValue b) {
			return SqlNull.Value;
		}

		public virtual ISqlValue XOr(ISqlValue x, ISqlValue y) {
			return SqlNull.Value;
		}

		#endregion

		#region Unary Operators

		public virtual ISqlValue Negate(ISqlValue value) {
			return SqlNull.Value;
		}

		public virtual ISqlValue UnaryPlus(ISqlValue value) {
			return SqlNull.Value;
		}

		public virtual ISqlValue Not(ISqlValue value) {
			return SqlNull.Value;
		}

		#endregion

		#endregion

		#region Parse

		public static SqlType Parse(IContext context, string sql) {
			ISqlTypeParser parser = null;
			if (context != null)
				parser = context.Scope.Resolve<ISqlTypeParser>();

			if (parser == null)
				throw new NotSupportedException("No data type parser was found in this context");

			return parser.Parse(context, sql);
		}

		public static SqlType Parse(string sql)
			=> Parse(null, sql);

		#endregion
	}
}