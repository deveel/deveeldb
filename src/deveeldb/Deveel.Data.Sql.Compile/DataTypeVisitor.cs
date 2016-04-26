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

using Antlr4.Runtime.Misc;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Compile {
	class DataTypeVisitor : PlSqlParserBaseVisitor<DataTypeInfo> {

		public override DataTypeInfo VisitIntervalType(PlSqlParser.IntervalTypeContext context) {
			if (context.DAY() != null &&
				context.TO() != null &&
				context.SECOND() != null)
				return new DataTypeInfo("DAY TO SECOND");

			if (context.YEAR() != null &&
				context.TO() != null &&
				context.MONTH() != null)
				return new DataTypeInfo("YEAR TO MONTH");

			return base.VisitIntervalType(context);
		}

		public override DataTypeInfo VisitUserDataType(PlSqlParser.UserDataTypeContext context) {
			var name = Name.Object(context.objectName());
			var args = context.type_argument();
			if (args != null && args.type_argument_spec().Length > 0) {
				// TODO:
			}

			return base.VisitUserDataType(context);
		}

		public override DataTypeInfo VisitInteger_type(PlSqlParser.Integer_typeContext context) {
			var size = Number.PositiveInteger(context.numeric()) ?? -1;
			SqlTypeCode typeCode;
			if (context.BIGINT() != null) {
				typeCode = SqlTypeCode.BigInt;
			} else if (context.INT() != null ||
			           context.INTEGER() != null) {
				typeCode = SqlTypeCode.Integer;
			} else if (context.SMALLINT() != null) {
				typeCode = SqlTypeCode.SmallInt;
			} else if (context.TINYINT() != null) {
				typeCode = SqlTypeCode.TinyInt;
			} else {
				throw new ParseCanceledException("Invalid integer type");
			}

			return new DataTypeInfo(typeCode.ToString().ToUpperInvariant(), new []{new DataTypeMeta("Size", size.ToString()) });
		}

		public override DataTypeInfo VisitNumeric_type(PlSqlParser.Numeric_typeContext context) {
			var precision = Number.PositiveInteger(context.precision) ?? -1;
			var scale = Number.PositiveInteger(context.scale) ?? -1;

			if (scale > 0 && precision <= 0)
				throw new ParseCanceledException("Invalid precision set.");
			if (scale > Byte.MaxValue - 1)
				throw new ParseCanceledException("Invalid scale value.");

			SqlTypeCode typeCode;

			if (context.DECIMAL() != null) {
				typeCode = SqlTypeCode.Decimal;
			} else if (context.REAL() != null) {
				typeCode = SqlTypeCode.Real;
			} else if (context.FLOAT() != null) {
				typeCode = SqlTypeCode.Float;
			} else if (context.DOUBLE() != null) {
				typeCode = SqlTypeCode.Double;
			} else if (context.NUMERIC() != null) {
				typeCode = SqlTypeCode.Numeric;
			} else {
				throw new ParseCanceledException("Invalid numeric type");
			}

			var meta = new[] {
				new DataTypeMeta("Precision", precision.ToString()),
				new DataTypeMeta("Scale", scale.ToString()),
			};

			return new DataTypeInfo(typeCode.ToString().ToUpperInvariant(), meta);
		}

		public override DataTypeInfo VisitString_type(PlSqlParser.String_typeContext context) {
			string size = null;
			if (context.numeric() != null) {
				size = Number.PositiveInteger(context.numeric()).ToString();
			} else if (context.MAX() != null) {
				size = "MAX";
			}

			SqlTypeCode typeCode;
			if (context.CHAR() != null) {
				typeCode = SqlTypeCode.Char;
			} else if (context.VARCHAR() != null) {
				typeCode = SqlTypeCode.VarChar;
			} else if (context.STRING() != null) {
				typeCode = SqlTypeCode.String;
			} else if (context.CLOB() != null) {
				typeCode = SqlTypeCode.Clob;
			} else if (!context.long_varchar().IsEmpty) {
				typeCode = SqlTypeCode.LongVarChar;
			} else {
				throw new ParseCanceledException("Invalid string type");
			}

			string encoding = null;
			if (context.ENCODING() != null)
				encoding = InputString.AsNotQuoted(context.encoding.Text);

			string locale = null;
			if (context.LOCALE() != null)
				locale = InputString.AsNotQuoted(context.locale.Text);

			var meta = new List<DataTypeMeta>();
			if (size != null)
				meta.Add(new DataTypeMeta("MaxSize", size));
			if (locale != null)
				meta.Add(new DataTypeMeta("Locale", locale));
			if (encoding != null)
				meta.Add(new DataTypeMeta("Encoding", encoding));

			return new DataTypeInfo(typeCode.ToString().ToUpperInvariant(), meta.ToArray());
		}

		public override DataTypeInfo VisitBinary_type(PlSqlParser.Binary_typeContext context) {
			var size = Number.PositiveInteger(context.numeric()) ?? -1;

			SqlTypeCode typeCode;

			if (context.BINARY() != null) {
				typeCode = SqlTypeCode.Binary;
			} else if (context.VARBINARY() != null) {
				typeCode = SqlTypeCode.VarBinary;
			} else if (context.BLOB() != null) {
				typeCode = SqlTypeCode.Blob;
			} else if (!context.long_varbinary().IsEmpty) {
				typeCode = SqlTypeCode.LongVarBinary;
			} else {
				throw new ParseCanceledException("Invalid binary type.");
			}

			return new DataTypeInfo(typeCode.ToString().ToUpperInvariant(), new[] {new DataTypeMeta("Size", size.ToString()) });
		}

		public override DataTypeInfo VisitBoolean_type(PlSqlParser.Boolean_typeContext context) {
			SqlTypeCode typeCode;
			if (context.BIT() != null) {
				typeCode = SqlTypeCode.Bit;
			} else if (context.BOOLEAN() != null) {
				typeCode = SqlTypeCode.Boolean;
			} else {
				throw new ParseCanceledException("Invalid boolean type.");
			}

			return new DataTypeInfo(typeCode.ToString().ToUpperInvariant());
		}

		public override DataTypeInfo VisitTime_type(PlSqlParser.Time_typeContext context) {
			SqlTypeCode typeCode;
			if (context.DATETIME() != null) {
				typeCode = SqlTypeCode.DateTime;
			} else if (context.DATE() != null) {
				typeCode = SqlTypeCode.Date;
			} else if (context.TIMESTAMP() != null) {
				typeCode = SqlTypeCode.TimeStamp;
			} else if (context.TIME() != null) {
				typeCode = SqlTypeCode.Time;
			} else {
				throw new ParseCanceledException("Invalid date type");
			}

			return new DataTypeInfo(typeCode.ToString().ToUpperInvariant());
		}
	}
}
