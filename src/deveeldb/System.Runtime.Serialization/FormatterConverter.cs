using System;
using System.Globalization;

namespace System.Runtime.Serialization {
	public class FormatterConverter : IFormatterConverter {
		public object Convert(object value, Type type) {
			return System.Convert.ChangeType(value, type, CultureInfo.InvariantCulture);
		}

#if !PCL
		public object Convert(object value, TypeCode typeCode) {
			throw new NotImplementedException();
		}
#endif

		public bool ToBoolean(object value) {
			return System.Convert.ToBoolean(value);
		}

		public byte ToByte(object value) {
			return System.Convert.ToByte(value);
		}

		public char ToChar(object value) {
			return System.Convert.ToChar(value);
		}

		public DateTime ToDateTime(object value) {
			return System.Convert.ToDateTime(value);
		}

		public decimal ToDecimal(object value) {
			return System.Convert.ToDecimal(value);
		}

		public double ToDouble(object value) {
			return System.Convert.ToDouble(value);
		}

		public short ToInt16(object value) {
			return System.Convert.ToInt16(value);
		}

		public int ToInt32(object value) {
			return System.Convert.ToInt32(value);
		}

		public long ToInt64(object value) {
			return System.Convert.ToInt64(value);
		}

		[CLSCompliant(false)]
		public sbyte ToSByte(object value) {
			return System.Convert.ToSByte(value);
		}

		public float ToSingle(object value) {
			return System.Convert.ToSingle(value);
		}

		public string ToString(object value) {
			return System.Convert.ToString(value);
		}

		[CLSCompliant(false)]
		public ushort ToUInt16(object value) {
			return System.Convert.ToUInt16(value);
		}

		[CLSCompliant(false)]
		public uint ToUInt32(object value) {
			return System.Convert.ToUInt32(value);
		}

		[CLSCompliant(false)]
		public ulong ToUInt64(object value) {
			return System.Convert.ToUInt64(value);
		}
	}
}
