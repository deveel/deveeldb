using System;

namespace System.Runtime.Serialization {
	public interface IFormatterConverter {
		object Convert(object value, Type type);

#if !PCL
		object Convert(object value, TypeCode typeCode);
#endif

		bool ToBoolean(object value);

		byte ToByte(object value);

		char ToChar(object value);

		DateTime ToDateTime(object value);

		decimal ToDecimal(object value);

		double ToDouble(object value);

		short ToInt16(object value);

		int ToInt32(object value);

		long ToInt64(object value);

		sbyte ToSByte(object value);

		float ToSingle(object value);

		string ToString(object value);

		ushort ToUInt16(object value);

		uint ToUInt32(object value);

		ulong ToUInt64(object value);
	}
}
