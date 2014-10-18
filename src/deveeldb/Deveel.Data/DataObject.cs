// 
//  Copyright 2010-2014 Deveel
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

using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

namespace Deveel.Data {
	[Serializable]
	public sealed class DataObject : IComparable, IComparable<DataObject>, IEquatable<DataObject> {
		public static readonly DataObject BooleanTrue = new DataObject(PrimitiveTypes.Boolean(), SqlBoolean.True);
		public static readonly DataObject BooleanFalse = new DataObject(PrimitiveTypes.Boolean(), SqlBoolean.False);
		public static readonly DataObject BooleanNull = new DataObject(PrimitiveTypes.Boolean(), SqlBoolean.Null);

		public DataObject(DataType type, ISqlObject value) {
			if (type == null)
				throw new ArgumentNullException("type");

			Type = type;
			Value = value;
		}

		public DataType Type { get; private set; }

		public ISqlObject Value { get; private set; }

		public bool IsNull {
			get { return Value == null || SqlNull.Value == Value || Value.IsNull; }
		}

		public bool IsComparableTo(DataObject obj) {
			return Type.IsComparable(obj.Type);
		}

		public int CompareTo(DataObject other) {
			// If this is null
			if (IsNull) {
				// and value is null return 0 return less
				if (other.IsNull)
					return 0;
				return -1;
			}
			// If this is not null and value is null return +1
			if (other.IsNull)
				return 1;

			// otherwise both are non null so compare normally.
			return CompareToNotNull(other);
		}

		private int CompareToNotNull(DataObject other) {
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
			return CompareTo((DataObject)obj);
		}

		public override int GetHashCode() {
			unchecked {
				var code = Type.GetHashCode()*23;
				if (Value != null)
					code = code ^ Value.GetHashCode();
				return code;
			}
		}

		public override bool Equals(object obj) {
			if (!(obj is DataObject))
				return false;

			return Equals((DataObject) obj);
		}

		public bool Equals(DataObject other) {
			if (other == null)
				return false;

			return IsEqualTo(other);
		}

		/// <summary>
		/// Compares to the given object to verify if is it compatible.
		/// </summary>
		/// <param name="other">The other object to verify.</param>
		/// <returns>
		/// Returns an instance of <see cref="DataObject"/> that defines
		/// if the given object is compatible with the current one.
		/// </returns>
		/// <seealso cref="IsComparableTo"/>
		/// <seealso cref="DataType.IsComparable"/>
		public DataObject Is(DataObject other) {
			if (IsNull && other.IsNull)
				return BooleanTrue;
			if (IsComparableTo(other))
				return Boolean(CompareTo(other) == 0);

			return BooleanFalse;
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
		/// Returns an instance of <see cref="DataObject"/> that defines
		/// if the given object is equal to the current one, or a boolean
		/// <c>null</c> if it was impossible to determine the types.
		/// </returns>
		/// <seealso cref="IsComparableTo"/>
		/// <seealso cref="DataType.IsComparable"/>
		public DataObject IsEqualTo(DataObject other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(CompareTo(other) == 0);

			return BooleanNull;
		}

		public DataObject IsNotEqualTo(DataObject other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(CompareTo(other) != 0);

			return BooleanNull;
		}

		public DataObject IsGreaterThan(DataObject other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(CompareTo(other) < 0);

			return BooleanNull;			
		}

		public DataObject IsSmallerThan(DataObject other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(CompareTo(other) > 0);

			return BooleanNull;
		}

		public DataObject IsGreterOrEqualThan(DataObject other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(CompareTo(other) <= 0);

			return BooleanNull;
		}

		public DataObject IsSmallerOrEqualThan(DataObject other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(CompareTo(other) >= 0);

			return BooleanNull;
		}

		public DataObject Negate() {
			if (IsNull)
				return this;

			if (Value is SqlBoolean)
				return Boolean(!(SqlBoolean) Value);
			if (Value is SqlNumber)
				return Number((NumericType) Type, -(SqlNumber) Value);

			return Null(Type);
		}

		#region Conversion

		public DataObject CastTo(DataType destType) {
			if (!Type.CanCastTo(destType))
				throw new InvalidCastException();

			return Type.CastTo(this, destType);
		}

		public DataObject ToBoolean() {
			return CastTo(PrimitiveTypes.Boolean());
		}

		public DataObject ToInteger() {
			return CastTo(PrimitiveTypes.Numeric(SqlTypeCode.Integer));
		}

		public DataObject ToBigInt() {
			return CastTo(PrimitiveTypes.Numeric(SqlTypeCode.BigInt));
		}

		#endregion

		#region Object Factory

		public static DataObject Boolean(bool value) {
			return new DataObject(PrimitiveTypes.Boolean(), new SqlBoolean(value));
		}

		public static DataObject Number(NumericType type, SqlNumber value) {
			return new DataObject(type, value);
		}

		public static DataObject Number(NumericType type, int value) {
			return new DataObject(type, new SqlNumber(value));
		}

		public static DataObject Number(NumericType type, long value) {
			return new DataObject(type, new SqlNumber(value));
		}

		public static DataObject Integer(int value) {
			return Number(PrimitiveTypes.Numeric(SqlTypeCode.Integer), value);
		}

		public static DataObject BigInt(long value) {
			return Number(PrimitiveTypes.Numeric(SqlTypeCode.BigInt), value);
		}

		public static DataObject String(string s) {
			return new DataObject(PrimitiveTypes.String(SqlTypeCode.String), new SqlString(s));
		}

		public static DataObject VarChar(string s) {
			return new DataObject(PrimitiveTypes.String(SqlTypeCode.VarChar), new SqlString(s));
		}

		public static DataObject Null(DataType type) {
			return new DataObject(type, SqlNull.Value);
		}

		#endregion

		#region Operators

		public static DataObject operator ==(DataObject a, DataObject b) {
			if (Equals(a, null) && Equals(b, null))
				return BooleanNull;
			if (Equals(a, null) || Equals(b, null))
				return BooleanNull;

			return a.IsEqualTo(b);
		}

		public static DataObject operator !=(DataObject a, DataObject b) {
			if (Equals(a, null) && Equals(b, null))
				return BooleanNull;
			if (Equals(a, null) || Equals(b, null))
				return BooleanNull;

			return a.IsNotEqualTo(b);
		}

		public static DataObject operator !(DataObject a) {
			return a.Negate();
		}

		#endregion

		#region Implicit Operators

		public static implicit operator bool(DataObject value) {
			if (Equals(value, null) || value.IsNull)
				throw new NullReferenceException();

			return (SqlBoolean) value.ToBoolean().Value;
		}

		public static implicit operator int(DataObject value) {
			if (Equals(value, null) || value.IsNull)
				throw new NullReferenceException();

			return ((SqlNumber)value.ToInteger().Value).ToInt32();
		}

		public static implicit operator long(DataObject value) {
			if (Equals(value, null) || value.IsNull)
				throw new NullReferenceException();

			return ((SqlNumber) value.ToBigInt().Value).ToInt64();
		}

		#endregion
	}
}