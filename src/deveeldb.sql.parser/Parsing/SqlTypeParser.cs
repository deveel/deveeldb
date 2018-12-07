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
using System.Collections.Generic;

using Antlr4.Runtime.Misc;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Parsing {
    static class SqlTypeParser {
        public static SqlType Parse(PlSqlParser.DatatypeContext context) {
            var typeInfo = GetResolveInfo(context);
            return PrimitiveTypes.Resolver.Resolve(typeInfo);
        }

        public static SqlTypeResolveInfo GetResolveInfo(PlSqlParser.DatatypeContext context) {
            return new SqlTypeVisitor().Visit(context);
        }

        #region SqlTypeVisitor

        class SqlTypeVisitor : PlSqlParserBaseVisitor<SqlTypeResolveInfo> {

            public override SqlTypeResolveInfo VisitIntervalType(PlSqlParser.IntervalTypeContext context) {
                if (context.DAY() != null &&
                    context.TO() != null &&
                    context.SECOND() != null)
                    return new SqlTypeResolveInfo("DAY TO SECOND");

                if (context.YEAR() != null &&
                    context.TO() != null &&
                    context.MONTH() != null)
                    return new SqlTypeResolveInfo("YEAR TO MONTH");

                return base.VisitIntervalType(context);
            }

            public override SqlTypeResolveInfo VisitUserDataType(PlSqlParser.UserDataTypeContext context) {
                var name = SqlParseUtil.Name.Object(context.objectName());
                var args = context.typeArgument();
                if (args != null && args.typeArgumentSpec().Length > 0) {
                    throw new NotSupportedException("Arguments to user-defined type are not supported yet.");
                }

                return new SqlTypeResolveInfo(name.FullName);
            }

            public override SqlTypeResolveInfo VisitIntegerType(PlSqlParser.IntegerTypeContext context) {
                var size = SqlParseUtil.PositiveInteger(context.numeric()) ?? -1;
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

                return new SqlTypeResolveInfo(typeCode.ToString().ToUpperInvariant(),
                    new Dictionary<string, object> {{"MaxSize", size}});
            }

            public override SqlTypeResolveInfo VisitNumericType(PlSqlParser.NumericTypeContext context) {
                var precision = SqlParseUtil.PositiveInteger(context.precision) ?? -1;
                var scale = SqlParseUtil.PositiveInteger(context.scale) ?? -1;

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

                var meta = new Dictionary<string, object> {
                    {"Precision", precision},
                    {"Scale", scale}
                };

                return new SqlTypeResolveInfo(typeCode.ToString().ToUpperInvariant(), meta);
            }

            public override SqlTypeResolveInfo VisitStringType(PlSqlParser.StringTypeContext context) {
                int? size = null;
                if (context.numeric() != null) {
                    size = SqlParseUtil.PositiveInteger(context.numeric());
                } else if (context.MAX() != null) {
                    size = SqlCharacterType.DefaultMaxSize;
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
                } else if (!context.longVarchar().IsEmpty) {
                    typeCode = SqlTypeCode.LongVarChar;
                } else {
                    throw new ParseCanceledException("Invalid string type");
                }

                string locale = null;
                if (context.LOCALE() != null)
                    locale = SqlParseInputString.AsNotQuoted(context.locale.Text);

                var meta = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                if (size != null) {
                    meta.Add("MaxSize", size);
                    meta.Add("Size", size);
                }
                if (locale != null)
                    meta.Add("Locale", locale);

                return new SqlTypeResolveInfo(typeCode.ToString().ToUpperInvariant(), meta);
            }

            public override SqlTypeResolveInfo VisitBinaryType(PlSqlParser.BinaryTypeContext context) {
                int? maxSize = null;
                if (context.MAX() != null) {
                    maxSize = SqlBinaryType.DefaultMaxSize;
                } else if (context.numeric() != null) {
                    maxSize = SqlParseUtil.PositiveInteger(context.numeric());
                }

                SqlTypeCode typeCode;

                if (context.BINARY() != null) {
                    typeCode = SqlTypeCode.Binary;
                } else if (context.VARBINARY() != null) {
                    typeCode = SqlTypeCode.VarBinary;
                } else if (context.BLOB() != null) {
                    typeCode = SqlTypeCode.Blob;
                } else if (!context.longVarbinary().IsEmpty) {
                    typeCode = SqlTypeCode.LongVarBinary;
                } else {
                    throw new ParseCanceledException("Invalid binary type.");
                }

                return new SqlTypeResolveInfo(typeCode.ToString().ToUpperInvariant(),
                    new Dictionary<string, object> {{"MaxSize", maxSize}, {"Size", maxSize}});
            }

            public override SqlTypeResolveInfo VisitBooleanType(PlSqlParser.BooleanTypeContext context) {
                SqlTypeCode typeCode;
                if (context.BIT() != null) {
                    typeCode = SqlTypeCode.Bit;
                } else if (context.BOOLEAN() != null) {
                    typeCode = SqlTypeCode.Boolean;
                } else {
                    throw new ParseCanceledException("Invalid boolean type.");
                }

                return new SqlTypeResolveInfo(typeCode.ToString().ToUpperInvariant());
            }

            public override SqlTypeResolveInfo VisitTimeType(PlSqlParser.TimeTypeContext context) {
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

                return new SqlTypeResolveInfo(typeCode.ToString().ToUpperInvariant());
            }

            public override SqlTypeResolveInfo VisitColumnRefType(PlSqlParser.ColumnRefTypeContext context) {
                var fieldName = SqlParseUtil.Name.Object(context.objectName());
                return new SqlTypeResolveInfo("%TYPE",
                    new Dictionary<string, object> {{"FieldName", fieldName.FullName}});
            }

            public override SqlTypeResolveInfo VisitRowRefType(PlSqlParser.RowRefTypeContext context) {
                var objName = SqlParseUtil.Name.Object(context.objectName());
                return new SqlTypeResolveInfo("%ROWTYPE",
                    new Dictionary<string, object> {{"ObjectName", objName.FullName}});
            }
        }


        #endregion
    }
}