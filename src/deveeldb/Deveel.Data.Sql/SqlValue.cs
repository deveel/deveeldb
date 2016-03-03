using System;
using System.IO;
using System.Text;

using Deveel.Data.Serialization;
using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	public abstract class SqlValue : ISerializable, IConvertible, IEquatable<SqlValue> {
		protected SqlValue(SqlType type) {
			if (type == null)
				throw new ArgumentNullException("type");

			Type = type;
		}

		protected SqlValue(ObjectData data) {
			Type = data.GetValue<SqlType>("Type");
		}

		public SqlType Type { get; private set; }

		public abstract bool IsNull { get; }

		internal bool IsCacheable {
			get { return Type.IsCacheable(this); }
		}

		internal int Size {
			get { return Type.ColumnSizeOf(this); }
		}

		public bool IsComparable {
			get { return this is IComparable; }
		}

		public bool IsComparableTo(SqlValue value) {
			return Type.IsComparable(value.Type);
		}

		public int CompareTo(SqlValue other) {
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
			return CompareToValue(other);
		}

		protected virtual int CompareToValue(SqlValue other) {
			if (!IsComparableTo(other))
				throw new NotSupportedException(String.Format("This object is not comparable to the other"));

			return Type.Compare(this, other);
		}

		public override int GetHashCode() {
			return base.GetHashCode();
		}

		public override bool Equals(object obj) {
			if (!(obj is SqlValue))
				return false;

			return Equals((SqlValue) obj);
		}

		public virtual bool Equals(SqlValue value) {
			var result = IsEqualTo(value);
			if (result.IsNull)
				return IsNull;

			return result;
		}

		void ISerializable.GetData(SerializeData data) {
			data.SetValue("Type", Type);
		}

		protected virtual void GetData(SerializeData data) {
			
		}

		#region Operations

		public SqlBoolean Is(SqlValue other) {
			if (IsNull && other.IsNull)
				return true;
			if (IsComparableTo(other))
				return CompareTo(other) == 0;

			return false;
		}

		public SqlBoolean IsNot(SqlValue other) {
			return (SqlBoolean) Is(other).Negate();
		}

		public SqlBoolean IsEqualTo(SqlValue other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Type.IsEqualTo(this, other);

			return null;
		}

		public SqlValue Negate() {
			if (IsNull)
				return this;

			return Type.Negate(this);
		}

		public SqlValue Plus() {
			if (IsNull)
				return this;

			return Type.UnaryPlus(this);
		}


		public SqlValue Reverse() {
			if (IsNull)
				return this;

			return Type.Reverse(this);
		}

		#endregion

		#region Runtime Conversions

		TypeCode IConvertible.GetTypeCode() {
			throw new NotImplementedException();
		}

		bool IConvertible.ToBoolean(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		sbyte IConvertible.ToSByte(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		byte IConvertible.ToByte(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		short IConvertible.ToInt16(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		ushort IConvertible.ToUInt16(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		int IConvertible.ToInt32(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		uint IConvertible.ToUInt32(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		long IConvertible.ToInt64(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		ulong IConvertible.ToUInt64(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		float IConvertible.ToSingle(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		double IConvertible.ToDouble(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		decimal IConvertible.ToDecimal(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		DateTime IConvertible.ToDateTime(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		string IConvertible.ToString(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		object IConvertible.ToType(Type conversionType, IFormatProvider provider) {
			throw new NotImplementedException();
		}

		#endregion
	}
}
