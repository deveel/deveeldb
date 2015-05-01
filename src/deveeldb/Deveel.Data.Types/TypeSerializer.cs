using System;
using System.Globalization;
using System.IO;
using System.Text;

namespace Deveel.Data.Types {
	static class TypeSerializer {
		public static void SerializeTo(Stream stream, DataType type) {
			var writer = new BinaryWriter(stream, Encoding.Unicode);
			writer.Write((byte) type.SqlType);

			if (type is NumericType) {
				var numericType = (NumericType) type;
				writer.Write(numericType.Size);
				writer.Write(numericType.Scale);
			} else if (type is StringType) {
				var stringType = (StringType) type;
				writer.Write(stringType.MaxSize);

				if (stringType.Locale != null) {
					writer.Write((byte) 1);
					writer.Write(stringType.Locale.LCID);
				} else {
					writer.Write((byte) 0);
				}
			} else if (type is BinaryType) {
				var binaryType = (BinaryType) type;

				writer.Write(binaryType.MaxSize);
			} else if (type is BooleanType ||
			           type is IntervalType ||
			           type is DateType ||
			           type is NullType) {
				// nothing to add to the SQL Type Code
			} else if (type is GeometryType) {
				var geometryType = (GeometryType) type;
				writer.Write(geometryType.Srid);
			} else {
				throw new NotSupportedException(String.Format("The data type '{0}' cannot be serialized.", type.GetType().FullName));
			}
		}

		public static DataType DeserializeFrom(Stream stream, IUserTypeResolver typeResolver) {
			var reader = new BinaryReader(stream, Encoding.Unicode);

			var typeCode = (SqlTypeCode) reader.ReadByte();

			if (BooleanType.IsBooleanType(typeCode))
				return PrimitiveTypes.Boolean(typeCode);
			if (IntervalType.IsIntervalType(typeCode))
				return PrimitiveTypes.Interval(typeCode);
			if (DateType.IsDateType(typeCode))
				return PrimitiveTypes.DateTime(typeCode);

			if (StringType.IsStringType(typeCode)) {
				var maxSize = reader.ReadInt32();

				CultureInfo locale = null;
				var hasLocale = reader.ReadByte() == 1;
				if (hasLocale) {
					var lcid = reader.ReadInt32();
					locale = CultureInfo.GetCultureInfo(lcid);
				}

				return PrimitiveTypes.String(typeCode, maxSize, locale);
			}

			if (NumericType.IsNumericType(typeCode)) {
				var size = reader.ReadInt32();
				var scale = reader.ReadByte();

				return PrimitiveTypes.Numeric(typeCode, size, scale);
			}

			if (BinaryType.IsBinaryType(typeCode)) {
				var size = reader.ReadInt32();
				return PrimitiveTypes.Binary(typeCode, size);
			}

			throw new NotSupportedException();
		}
	}
}
