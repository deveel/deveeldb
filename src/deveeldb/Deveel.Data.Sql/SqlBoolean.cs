using System;
using System.Globalization;

using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class SqlBoolean : SqlValue, IComparable<SqlBoolean>, IComparable {
		private readonly byte? value;

		public static readonly SqlBoolean Null = new SqlBoolean(new BooleanType(SqlTypeCode.Boolean), null);

		public SqlBoolean(byte value)
			: this(new BooleanType(SqlTypeCode.Boolean), value) {
		}

		public SqlBoolean(BooleanType type, byte value)
			: this(type, (byte?) value) {
		}

		private SqlBoolean(BooleanType type, byte? value)
			: base(type) {
			if (value != null) {
				if (value != 0 &&
					value != 1)
					throw new ArgumentOutOfRangeException("value", "Invalid boolean value: can be just 0 or 1");
			}

			this.value = value;
		}

		public SqlBoolean(bool value)
			: this(new BooleanType(SqlTypeCode.Boolean), value) {
		}

		public SqlBoolean(BooleanType type, bool value)
			: this(type, (byte) (value ? 1 : 0)) {
		}

		public override bool IsNull {
			get { return value == null; }
		}

		#region Implicit Conversion

		public static implicit operator bool(SqlBoolean value) {
			return (value as IConvertible).ToBoolean(CultureInfo.InvariantCulture);
		}

		public static implicit operator SqlBoolean(bool value) {
			return new SqlBoolean(value);
		}

		public static implicit operator byte(SqlBoolean value) {
			return (value as IConvertible).ToByte(CultureInfo.InvariantCulture);
		}

		public static implicit operator SqlBoolean(byte value) {
			return new SqlBoolean(value);
		}

		public static implicit operator SqlBoolean(DBNull value) {
			return Null;
		}

		#endregion

		public int CompareTo(SqlBoolean other) {
			throw new NotImplementedException();
		}

		int IComparable.CompareTo(object obj) {
			if (!(obj is SqlBoolean))
				throw new ArgumentException();

			return CompareTo((SqlBoolean) obj);
		}
	}
}
