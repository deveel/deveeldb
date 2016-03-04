using System;
using System.Runtime.Serialization;

namespace Deveel.Data.Serialization {
	internal class BinaryCommon {
		// Header present in all binary serializations
		public static byte[] BinaryHeader = new Byte[] { 0, 1, 0, 0, 0, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0, 0, 0 };

		static Type[] _typeCodesToType;

		static BinaryCommon() {
			_typeCodesToType = new Type[19];
			_typeCodesToType[(int)BinaryTypeCode.Boolean] = typeof(Boolean);
			_typeCodesToType[(int)BinaryTypeCode.Byte] = typeof(Byte);
			_typeCodesToType[(int)BinaryTypeCode.Char] = typeof(Char);
			_typeCodesToType[(int)BinaryTypeCode.TimeSpan] = typeof(TimeSpan);
			_typeCodesToType[(int)BinaryTypeCode.DateTime] = typeof(DateTime);
			_typeCodesToType[(int)BinaryTypeCode.Decimal] = typeof(Decimal);
			_typeCodesToType[(int)BinaryTypeCode.Double] = typeof(Double);
			_typeCodesToType[(int)BinaryTypeCode.Int16] = typeof(Int16);
			_typeCodesToType[(int)BinaryTypeCode.Int32] = typeof(Int32);
			_typeCodesToType[(int)BinaryTypeCode.Int64] = typeof(Int64);
			_typeCodesToType[(int)BinaryTypeCode.SByte] = typeof(SByte);
			_typeCodesToType[(int)BinaryTypeCode.Single] = typeof(Single);
			_typeCodesToType[(int)BinaryTypeCode.UInt16] = typeof(UInt16);
			_typeCodesToType[(int)BinaryTypeCode.UInt32] = typeof(UInt32);
			_typeCodesToType[(int)BinaryTypeCode.UInt64] = typeof(UInt64);
			_typeCodesToType[(int)BinaryTypeCode.Null] = null;
			_typeCodesToType[(int)BinaryTypeCode.String] = typeof(string);
			// TimeStamp does not have a TypeCode, so it is managed as a special
			// case in GetTypeCode()
		}

		public static bool IsPrimitive(Type type) {
			return (type.IsPrimitive && type != typeof(IntPtr)) ||
				type == typeof(DateTime) ||
				type == typeof(TimeSpan) ||
				type == typeof(Decimal);
		}

		public static Type GetTypeFromCode(int code) {
			return _typeCodesToType[code];
		}


		public static void SwapBytes(byte[] byteArray, int size, int dataSize) {
			byte b;
			if (dataSize == 8) {
				for (int n = 0; n < size; n += 8) {
					b = byteArray[n]; byteArray[n] = byteArray[n + 7]; byteArray[n + 7] = b;
					b = byteArray[n + 1]; byteArray[n + 1] = byteArray[n + 6]; byteArray[n + 6] = b;
					b = byteArray[n + 2]; byteArray[n + 2] = byteArray[n + 5]; byteArray[n + 5] = b;
					b = byteArray[n + 3]; byteArray[n + 3] = byteArray[n + 4]; byteArray[n + 4] = b;
				}
			} else if (dataSize == 4) {
				for (int n = 0; n < size; n += 4) {
					b = byteArray[n]; byteArray[n] = byteArray[n + 3]; byteArray[n + 3] = b;
					b = byteArray[n + 1]; byteArray[n + 1] = byteArray[n + 2]; byteArray[n + 2] = b;
				}
			} else if (dataSize == 2) {
				for (int n = 0; n < size; n += 2) {
					b = byteArray[n]; byteArray[n] = byteArray[n + 1]; byteArray[n + 1] = b;
				}
			}
		}
	}
}
