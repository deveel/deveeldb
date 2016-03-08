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
using System.IO;
using System.Runtime.Serialization;

using Deveel.Data;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using SqlBoolean = Deveel.Data.Sql.Objects.SqlBoolean;
using SqlString = Deveel.Data.Sql.Objects.SqlString;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class Field : IComparable, IComparable<Field>, IEquatable<Field>, ISerializable {
		/// <summary>
		/// The representation of a BOOLEAN <c>true</c> as <see cref="Field"/>
		/// </summary>
		public static readonly Field BooleanTrue = new Field(PrimitiveTypes.Boolean(), SqlBoolean.True);

		/// <summary>
		/// The representation of a BOOLEAN <c>false</c> as <see cref="Field"/>
		/// </summary>
		public static readonly Field BooleanFalse = new Field(PrimitiveTypes.Boolean(), SqlBoolean.False);

		/// <summary>
		/// The <c>null</c> representation of a BOOLEAN object.
		/// </summary>
		public static readonly Field BooleanNull = new Field(PrimitiveTypes.Boolean(), SqlBoolean.Null);

		/// <summary>
		/// Constructs a new database data object with a specific <see cref="SqlType"/> 
		/// and handling the specified <see cref="ISqlObject"/> value.
		/// </summary>
		/// <param name="type">The specific <see cref="SqlType"/> that is used by this object
		/// to shape the data and compute operations.</param>
		/// <param name="value">The innermost value of the object to be handled.</param>
		/// <exception cref="ArgumentNullException">
		/// If the specified <paramref name="type"/> is <c>null</c>.
		/// </exception>
		public Field(SqlType type, ISqlObject value) {
			if (type == null)
				throw new ArgumentNullException("type");

			Type = type;
			Value = value;
		}

		private Field(SerializationInfo info, StreamingContext context) {
			Type = (SqlType)info.GetValue("Type", typeof(SqlType));
			Value = (ISqlObject) info.GetValue("Value", typeof(ISqlObject));
		}

		/// <summary>
		/// Gets the <see cref="SqlType"/> that defines the object properties
		/// </summary>
		/// <seealso cref="SqlType"/>
		public SqlType Type { get; private set; }

		/// <summary>
		/// Gets the underlined <see cref="ISqlObject">value</see> that is handled.
		/// </summary>
		/// <seealso cref="ISqlObject"/>
		public ISqlObject Value { get; private set; }

		/// <summary>
		/// Gets a value that indicates if this object is materialized as <c>null</c>.
		/// </summary>
		/// <seealso cref="SqlNull.Value"/>
		/// <seealso cref="ISqlObject.IsNull"/>
		public bool IsNull {
			get {
				return Type.IsNull ||
				       Value == null ||
				       SqlNull.Value == Value ||
				       Value.IsNull;
			}
		}

		internal int CacheUsage {
			get { return Type.GetCacheUsage(Value); }
		}

		internal bool IsCacheable {
			get { return Type.IsCacheable(Value); }
		}

		internal int Size {
			get { return Type.ColumnSizeOf(Value); }
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Type", Type, typeof(SqlType));
			info.AddValue("Value", Value, typeof(ISqlObject));
		}

		/// <summary>
		/// Checks if the given <see cref="Field">object</see> is comparable
		/// to this object,
		/// </summary>
		/// <param name="obj">The object to compare.</param>
		/// <returns>
		/// Returns <c>true</c> if the given object is comparable to this object,
		/// or <c>false</c> otherwise.
		/// </returns>
		public bool IsComparableTo(Field obj) {
			return Type.IsComparable(obj.Type);
		}

		/// <inheritdoc/>
		public int CompareTo(Field other) {
			// If this is null
			if (IsNull) {
				// and value is null return 0 return less
				if (other.IsNull)
					return 0;
				return -1;
			}
			// If this is not null and value is null return +1
			if (ReferenceEquals(null, other) || 
				other.IsNull)
				return 1;

			// otherwise both are non null so compare normally.
			return CompareToNotNull(other);
		}

		private int CompareToNotNull(Field other) {
			var type = Type;
			// Strings must be handled as a special case.
			if (type is StringType) {
				// We must determine the locale to compare against and use that.
				var stype = (StringType)type;
				// If there is no locale defined for this type we use the locale in the
				// given type.
				if (stype.Locale == null) {
					type = other.Type;
				}
			}
			return type.Compare(Value, other.Value);

		}

		int IComparable.CompareTo(object obj) {
			return CompareTo((Field)obj);
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			unchecked {
				var code = Type.GetHashCode()*23;
				if (Value != null)
					code = code ^ Value.GetHashCode();
				return code;
			}
		}

		/// <inheritdoc/>
		public override bool Equals(object obj) {
			if (!(obj is Field))
				return false;

			return Equals((Field) obj);
		}

		/// <inheritdoc/>
		public bool Equals(Field other) {
			if (ReferenceEquals(other, null))
				return IsNull;

			var result = IsEqualTo(other);
			if (result.IsNull)
				return IsNull;

			return result.AsBoolean();
		}

		/// <summary>
		/// Compares to the given object to verify if is it compatible.
		/// </summary>
		/// <param name="other">The other object to verify.</param>
		/// <returns>
		/// Returns an instance of <see cref="Field"/> that defines
		/// if the given object is compatible with the current one.
		/// </returns>
		/// <seealso cref="IsComparableTo"/>
		/// <seealso cref="SqlType.IsComparable"/>
		public Field Is(Field other) {
			if (IsNull && other.IsNull)
				return BooleanTrue;
			if (IsComparableTo(other))
				return Boolean(CompareTo(other) == 0);

			return BooleanFalse;
		}

		/// <summary>
		/// Compares the given object to verify if it is not compatible with
		/// this one.
		/// </summary>
		/// <param name="other">The other object to compare.</param>
		/// <remarks>
		/// This method is equivalent to calling <see cref="Is"/> and
		/// then <see cref="Negate"/> to obtain the inverse value.
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="Field"/> that defines
		/// if the given object is not compatible with the current one.
		/// </returns>
		/// <seealso cref="Is"/>
		/// <seealso cref="Negate"/>
		public Field IsNot(Field other) {
			return Is(other).Negate();
		}

		/// <summary>
		/// Compares to the given object to verify if is it equal to the current.
		/// </summary>
		/// <param name="other">The other object to verify.</param>
		/// <remarks>
		/// This method returns a boolean value of <c>true</c> or <c>false</c>
		/// only if the current object and the other object are not <c>null</c>.
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="Field"/> that defines
		/// if the given object is equal to the current one, or a boolean
		/// <c>null</c> if it was impossible to determine the types.
		/// </returns>
		/// <seealso cref="IsComparableTo"/>
		/// <seealso cref="SqlType.IsComparable"/>
		public Field IsEqualTo(Field other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(Type.IsEqualTo(Value, other.Value));

			return BooleanNull;
		}

		/// <summary>
		/// Compares to the given object to verify if is it not equal to the current.
		/// </summary>
		/// <param name="other">The other object to compare.</param>
		/// <remarks>
		/// This method returns a boolean value of <c>true</c> or <c>false</c>
		/// only if the current object and the other object are not <c>null</c>.
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="Field"/> that defines
		/// if the given object is equal to the current one, or a boolean
		/// <c>null</c> if it was impossible to determine the types.
		/// </returns>
		/// <seealso cref="IsComparableTo"/>
		/// <seealso cref="SqlType.IsComparable"/>
		public Field IsNotEqualTo(Field other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(Type.IsNotEqualTo(Value, other.Value));

			return BooleanNull;
		}

		public Field IsGreaterThan(Field other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(Type.IsGreatherThan(Value, other.Value));

			return BooleanNull;			
		}

		public Field IsSmallerThan(Field other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(Type.IsSmallerThan(Value, other.Value));

			return BooleanNull;
		}

		public Field IsGreterOrEqualThan(Field other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(Type.IsGreaterOrEqualThan(Value, other.Value));

			return BooleanNull;
		}

		public Field IsSmallerOrEqualThan(Field other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(Type.IsSmallerOrEqualThan(Value, other.Value));

			return BooleanNull;
		}

		/// <summary>
		/// When the type of this object is a string, this method verifies if the
		/// input pattern is compatible (<<c>likes</c>) with the input. 
		/// </summary>
		/// <param name="pattern">The input string object pattern used to verify
		/// the likeness with the underlying string object..</param>
		/// <returns>
		/// <remarks>
		/// This operation can be computed only if <see cref="Type"/> represents a
		/// <see cref="StringType"/> and the input <paramref name="pattern"/> also.
		/// </remarks>
		/// Returns an instance of <see cref="Field"/> that represents a
		/// <c>true</c> or <c>false</c> if the underlying string value matches or not 
		/// the provided pattern. If this object or the provided pattern are not strings,
		/// this method returns a boolean <c>null</c>.
		/// </returns>
		public Field IsLike(Field pattern) {
			if (IsNull || !(Type is StringType))
				return BooleanNull;

			if (!(pattern.Type is StringType) ||
			    pattern.IsNull)
				return BooleanNull;

			var valueString = (ISqlString) Value;
			var patternString = (ISqlString) pattern.Value;
			return Boolean((Type as StringType).IsLike(valueString, patternString));
		}

		public Field IsNotLike(Field pattern) {
			if (IsNull || !(Type is StringType))
				return BooleanNull;

			var valueString = (ISqlString)Value;
			var patternString = (ISqlString)pattern.Value;
			return Boolean((Type as StringType).IsNotLike(valueString, patternString));
		}

		/// <summary>
		/// Negates the current underlying value of the object.
		/// </summary>
		/// <remarks>
		/// The value negation is delegated to the underlying <see cref="SqlType"/>
		/// implementation set to this object: this means not all the objects
		/// will handle negation, but instead they will return a <seealso cref="SqlNull"/> value.
		/// </remarks>
		/// <returns>
		/// This returns an instance of <see cref="Field"/> whose
		/// <see cref="Value"/> is the negation of the current handled value.
		/// </returns>
		/// <seealso cref="SqlType.Negate"/>
		public Field Negate() {
			if (IsNull)
				return this;

			return new Field(Type, Type.Negate(Value));
		}

		public Field Plus() {
			if (IsNull)
				return this;
			
			return new Field(Type, Type.UnaryPlus(Value));
		}

		/// <summary>
		/// Adds the given value to this object value.
		/// </summary>
		/// <param name="other">The object that handles the value
		/// to be added to this one.</param>
		/// <returns>
		/// Returns an instance of <see cref="Field"/> that is the result of
		/// he addition of this value to the provided value, or <c>null</c> if
		/// this object or the other object <see cref="IsNull">is null</see>.
		/// </returns>
		public Field Add(Field other) {
			if (IsNull)
				return this;

			var widerType = Type.Wider(other.Type);
			var result = widerType.Add(Value, other.Value);
			return new Field(widerType, result);
		}

		public Field Subtract(Field other) {
			if (IsNull)
				return this;

			var widerType = Type.Wider(other.Type);
			var result = widerType.Subtract(Value, other.Value);
			return new Field(widerType, result);
		}

		public Field Multiply(Field other) {
			if (IsNull)
				return this;

			var widerType = Type.Wider(other.Type);
			var result = widerType.Multiply(Value, other.Value);
			return new Field(widerType, result);
		}

		public Field Divide(Field other) {
			if (IsNull)
				return this;

			var widerType = Type.Wider(other.Type);
			var result = widerType.Divide(Value, other.Value);
			return new Field(widerType, result);
		}

		public Field Modulus(Field other) {
			if (IsNull)
				return this;

			var widerType = Type.Wider(other.Type);
			var result = widerType.Modulus(Value, other.Value);
			return new Field(widerType, result);			
		}

		public Field Or(Field other) {
			if (IsNull)
				return this;

			var widerType = Type.Wider(other.Type);
			var result = widerType.Or(Value, other.Value);
			return new Field(widerType, result);
		}

		public Field And(Field other) {
			if (IsNull)
				return this;

			var widerType = Type.Wider(other.Type);
			var result = widerType.And(Value, other.Value);
			return new Field(widerType, result);
		}

		public Field XOr(Field other) {
			if (IsNull)
				return this;

			var widerType = Type.Wider(other.Type);
			var result = widerType.XOr(Value, other.Value);
			return new Field(widerType, result);
		}

		public Field Any(SqlExpressionType type, Field other, EvaluateContext context) {
			if (IsNull)
				return this;

			return GroupOperatorHelper.EvaluateAny(type, this, other, context);
		}

		public Field All(SqlExpressionType type, Field other, EvaluateContext context) {
			if (IsNull)
				return this;

			return GroupOperatorHelper.EvaluateAll(type, this, other, context);
		}

		public Field Reverse() {
			if (IsNull)
				return this;
			
			return new Field(Type, Type.Reverse(Value));
		}

		#region Conversion

		/// <summary>
		/// Converts this object to the given <see cref="SqlType"/>.
		/// </summary>
		/// <param name="destType">The destination <see cref="SqlType"/> to cast this
		/// object to.</param>
		/// <returns>
		/// Returns an instance of <see cref="Field"/> that has a <see cref="Type"/>
		/// equals to the given <see cref="SqlType"/> and <see cref="Value"/> as a
		/// <see cref="ISqlObject"/> compatible with the given type.
		/// </returns>
		public Field CastTo(SqlType destType) {
			if (!Type.CanCastTo(destType))
				throw new InvalidCastException();

			if (Type.Equals(destType))
				return this;

			var casted = Type.CastTo(Value, destType);
			return new Field(destType, casted);
		}

		/// <summary>
		/// Converts this object to a boolean type.
		/// </summary>
		/// <remarks>
		/// This method is a shortcut to the original <see cref="CastTo"/>
		/// method with a <see cref="BooleanType"/> parameter.
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="Field"/> that is compatible
		/// with a boolean type.
		/// </returns>
		/// <seealso cref="CastTo"/>
		/// <seealso cref="PrimitiveTypes.Boolean()"/>
		/// <seealso cref="BooleanType"/>
		public Field AsBoolean() {
			return CastTo(PrimitiveTypes.Boolean());
		}

		public Field AsTinyInt() {
			return CastTo(PrimitiveTypes.TinyInt());
		}

		public Field AsInteger() {
			return CastTo(PrimitiveTypes.Numeric(SqlTypeCode.Integer));
		}

		public Field AsBigInt() {
			return CastTo(PrimitiveTypes.Numeric(SqlTypeCode.BigInt));
		}

		public Field AsVarChar() {
			return CastTo(PrimitiveTypes.String(SqlTypeCode.VarChar));
		}

		public Field AsDate() {
			return CastTo(PrimitiveTypes.Date());
		}

		public Field AsTimeStamp() {
			return CastTo(PrimitiveTypes.TimeStamp());
		}

		#endregion

		#region Object provider

		public static Field Boolean(SqlBoolean value) {
			return new Field(PrimitiveTypes.Boolean(), value);
		}

		public static Field Boolean(bool value) {
			return Boolean((SqlBoolean)value);
		}

		public static Field Number(SqlNumber value) {
			return Number(PrimitiveTypes.Numeric(), value);
		}

		public static Field Number(NumericType type, SqlNumber value) {
			return new Field(type, value);
		}

		public static Field Number(NumericType type, int value) {
			return new Field(type, new SqlNumber(value));
		}

		public static Field Number(NumericType type, long value) {
			return new Field(type, new SqlNumber(value));
		}

		public static Field TinyInt(byte value) {
			return Number(PrimitiveTypes.Numeric(SqlTypeCode.TinyInt), value);
		}

		public static Field SmallInt(short value) {
			return Number(PrimitiveTypes.Numeric(SqlTypeCode.SmallInt), value);
		}

		public static Field Integer(int value) {
			return Number(PrimitiveTypes.Numeric(SqlTypeCode.Integer), value);
		}

		public static Field BigInt(long value) {
			return Number(PrimitiveTypes.Numeric(SqlTypeCode.BigInt), value);
		}

		public static Field Float(float value) {
			return Number(PrimitiveTypes.Numeric(SqlTypeCode.Float), new SqlNumber(value));
		}

		public static Field Double(double value) {
			return Number(PrimitiveTypes.Numeric(SqlTypeCode.Double), new SqlNumber(value));
		}

		public static Field String(string s) {
			return String(new SqlString(s));
		}

		public static Field String(SqlString s) {
			return new Field(PrimitiveTypes.String(SqlTypeCode.String), s);
		}

		public static Field Date(DateTimeOffset value) {
			var offset = new SqlDayToSecond(value.Offset.Days, value.Offset.Hours, value.Offset.Minutes, value.Offset.Seconds, value.Offset.Milliseconds);
			var sqlDate = new SqlDateTime(value.Year, value.Month, value.Day, value.Hour, value.Minute, value.Second, value.Millisecond, offset);
			return Date(sqlDate);
		}

		public static Field Date(SqlDateTime value) {
			return Date(SqlTypeCode.Date, value);
		}

		public static Field TimeStamp(SqlDateTime value) {
			return Date(SqlTypeCode.TimeStamp, value);
		}

		public static Field Date(SqlTypeCode typeCode, SqlDateTime value) {
			return new Field(PrimitiveTypes.DateTime(typeCode), value);
		}

		public static Field Time(SqlDateTime value) {
			return Date(SqlTypeCode.Time, value);
		}

		public static Field VarChar(string s) {
			return VarChar(new SqlString(s));
		}

		public static Field VarChar(SqlString s) {
			return new Field(PrimitiveTypes.String(SqlTypeCode.VarChar), s);
		}

		public static Field Null(SqlType type) {
			return new Field(type, SqlNull.Value);
		}

		public static Field Null() {
			return Null(new NullType(SqlTypeCode.Null));
		}

		public static Field Binary(SqlBinary binary) {
			return new Field(new BinaryType(SqlTypeCode.Binary), binary);
		}

		public static Field Binary(byte[] binary) {
			return Binary(new SqlBinary(binary));
		}

		public static Field Create(object value) {
			// Numeric values ...
			if (value is bool)
				return Boolean((bool) value);
			if (value is byte)
				return TinyInt((byte) value);
			if (value is short)
				return SmallInt((short) value);
			if (value is int)
				return Integer((int) value);
			if (value is long)
				return BigInt((long) value);
			if (value is float)
				return Float((float) value);
			if (value is double)
				return Double((double) value);

			if (value is SqlNumber) {
				var num = (SqlNumber) value;
				if (num.IsNull)
					return Null(PrimitiveTypes.Numeric());

				if (num.CanBeInt32)
					return Integer(num.ToInt32());
				if (num.CanBeInt64)
					return BigInt(num.ToInt64());

				return Number(num);
			}

			// String values ...
			if (value is string)
				return String((string) value);
			if (value is SqlString) {
				var s = (SqlString) value;
				if (s.IsNull)
					return Null(PrimitiveTypes.String());

				return String(s);
			}

			throw new NotSupportedException("Cannot build an object from the given value.");
		}

		#endregion

		#region Operators

		/// <summary>
		/// The equality operation between two <see cref="Field"/> instances.
		/// </summary>
		/// <param name="a">The first operand.</param>
		/// <param name="b">The second operand.</param>
		/// <returns>
		/// Returns an instance of <see cref="Field"/> that indicates the
		/// boolean equality state of the two operands provided.
		/// </returns>
		/// <seealso cref="IsEqualTo"/>
		public static Field operator ==(Field a, Field b) {
			if (Equals(a, null) && Equals(b, null))
				return BooleanNull;
			if (Equals(a, null) || Equals(b, null))
				return BooleanNull;

			return a.IsEqualTo(b);
		}

#if !PCL
		public static Field operator ==(Field a, DBNull b) {
			if (Equals(a, null) || a.IsNull)
				return BooleanTrue;

			return BooleanFalse;
		}

		public static Field operator !=(Field a, DBNull b) {
			return !(a == b);
		}

#endif

		/// <summary>
		/// The inequality operation between two <see cref="Field"/> instances.
		/// </summary>
		/// <param name="a">The first operand.</param>
		/// <param name="b">The second operand.</param>
		/// <returns>
		/// Returns an instance of <see cref="Field"/> that indicates the
		/// boolean inequality state of the two operands provided.
		/// </returns>
		/// <seealso cref="IsNotEqualTo"/>
		public static Field operator !=(Field a, Field b) {
			if (Equals(a, null) && Equals(b, null))
				return BooleanNull;
			if (Equals(a, null) || Equals(b, null))
				return BooleanNull;

			return a.IsNotEqualTo(b);
		}

		/// <summary>
		/// The addition operator between two numeric values.
		/// </summary>
		/// <param name="a">The first operand.</param>
		/// <param name="b">The second operand.</param>
		/// <returns>
		/// Returns an instance of <see cref="Field"/> that is the
		/// numeric result of the addition of the two operands
		/// </returns>
		/// <seealso cref="Add"/>
		public static Field operator +(Field a, Field b) {
			if (Equals(a, null) && Equals(b, null))
				return Null();
			if (Equals(a, null))
				return Null();

			return a.Add(b);
		}

		/// <summary>
		/// The subtraction operator between two numeric values.
		/// </summary>
		/// <param name="a">The first operand.</param>
		/// <param name="b">The second operand.</param>
		/// <returns>
		/// Returns an instance of <see cref="Field"/> that is the
		/// numeric result of the subtraction of the two operands
		/// </returns>
		/// <seealso cref="Subtract"/>
		public static Field operator -(Field a, Field b) {
			if (Equals(a, null) && Equals(b, null))
				return Null();
			if (Equals(a, null))
				return Null();

			return a.Subtract(b);
		}

		public static Field operator /(Field a, Field b) {
			if (Equals(a, null) && Equals(b, null))
				return Null();
			if (Equals(a, null))
				return Null();

			return a.Divide(b);
		}

		public static Field operator *(Field a, Field b) {
			if (Equals(a, null) && Equals(b, null))
				return Null();
			if (Equals(a, null))
				return Null();

			return a.Multiply(b);
		}

		public static Field operator %(Field a, Field b) {
			if (Equals(a, null) && Equals(b, null))
				return Null();
			if (Equals(a, null))
				return Null();

			return a.Modulus(b);
		}

		public static Field operator &(Field a, Field b) {
			if (Equals(a, null) && Equals(b, null))
				return Null();
			if (Equals(a, null))
				return Null();

			return a.And(b);
		}

		public static bool operator true(Field a) {
			return a.IsEqualTo(Field.BooleanTrue);
		}

		public static bool operator false(Field a) {
			return a.IsEqualTo(Field.BooleanFalse);
		}

		public static Field operator |(Field a, Field b) {
			if (Equals(a, null) && Equals(b, null))
				return Null();
			if (Equals(a, null))
				return Null();

			return a.Or(b);
		}

		public static Field operator ^(Field a, Field b) {
			if (Equals(a, null) && Equals(b, null))
				return Null();
			if (Equals(a, null))
				return Null();

			return a.XOr(b);
		}

		public static Field operator !(Field value) {
			return value.Negate();
		}

		public static Field operator -(Field value) {
			return value.Negate();
		}

		public static Field operator ~(Field value) {
			return value.Reverse();
		}

		public static Field operator +(Field value) {
			return value.Plus();
		}

		#endregion

		#region Implicit Operators

		public static implicit operator bool(Field value) {
			if (ReferenceEquals(value, null) || value.IsNull)
				throw new InvalidCastException("Cannot convert a NULL value to a boolean.");

			return (SqlBoolean) value.AsBoolean().Value;
		}

		public static implicit operator int(Field value) {
			if (ReferenceEquals(value, null) || value.IsNull)
				throw new InvalidCastException("Cannot convert NULL value to integer.");

			return ((SqlNumber)value.AsInteger().Value).ToInt32();
		}

		public static implicit operator long(Field value) {
			if (ReferenceEquals(value, null) || value.IsNull)
				throw new InvalidCastException("Cannot convert NULL to long integer");

			return ((SqlNumber) value.AsBigInt().Value).ToInt64();
		}

		public static implicit operator string(Field value) {
			if (ReferenceEquals(value, null) || value.IsNull)
				return null;

			return ((SqlString) value.AsVarChar().Value).Value;
		}

		public static implicit operator DateTime?(Field value) {
			if (ReferenceEquals(value, null) || value.IsNull)
				return null;

			return ((SqlDateTime) value.AsDate().Value).ToDateTime();
		}

		public static implicit operator DateTime(Field value) {
			if (ReferenceEquals(value, null) || value.IsNull)
				throw new InvalidCastException("Cannot convert NULL value to a non-nullable date time.");

			return ((SqlDateTime)value.AsDate().Value).ToDateTime();
		}

#if !PCL
		public static implicit operator DBNull(Field value) {
			if (ReferenceEquals(value, null) || value.IsNull)
				return DBNull.Value;

			throw new InvalidCastException("Cannot convert non-nullable value to DBNull.");
		}

#endif

		#endregion

		public void SerializeValueTo(Stream stream, ISystemContext systemContext) {
			Type.SerializeObject(stream, Value);
		}
	}
}