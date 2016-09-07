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
using System.Data;
using System.Data.Common;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Client {
	public sealed class DeveelDbParameter : DbParameter, IDbDataParameter {
		private SqlTypeCode typeCode;
		private DbType dbType;

		public DeveelDbParameter() {
		}

		public DeveelDbParameter(string parameterName) {
			ParameterName = parameterName;
		}

		public DeveelDbParameter(string parameterName, DbType dbType) {
			ParameterName = parameterName;
			DbType = dbType;
		}

		public DeveelDbParameter(string parameterName, DbType dbType, object value) {
			ParameterName = parameterName;
			Value = value;
			DbType = dbType;
		}

		public DeveelDbParameter(string parameterName, object value) {
			ParameterName = parameterName;
			Value = value;
			DbType = DiscoverDbType(value);
		}

		public override void ResetDbType() {
			switch (typeCode) {
				case SqlTypeCode.Bit:
				case SqlTypeCode.Boolean:
					dbType = DbType.Boolean;
					break;
				case SqlTypeCode.TinyInt:
					dbType = DbType.Byte;
					break;
				case SqlTypeCode.SmallInt:
					dbType = DbType.Int16;
					break;
				case SqlTypeCode.Integer:
					dbType = DbType.Int32;
					break;
				case SqlTypeCode.BigInt:
					dbType = DbType.Int64;
					break;
				case SqlTypeCode.Real:
				case SqlTypeCode.Float:
					dbType = DbType.Single;
					break;
				case SqlTypeCode.Double:
					dbType = DbType.Double;
					break;
				case SqlTypeCode.Decimal:
					dbType = DbType.Decimal;
					break;
				case SqlTypeCode.Numeric:
					dbType = DbType.VarNumeric;
					break;
				case SqlTypeCode.String:
				case SqlTypeCode.VarChar:
				case SqlTypeCode.Clob:
				case SqlTypeCode.LongVarChar:
					dbType = DbType.String;
					break;
				case SqlTypeCode.Char:
					dbType = DbType.StringFixedLength;
					break;
				case SqlTypeCode.VarBinary:
				case SqlTypeCode.Blob:
				case SqlTypeCode.LongVarBinary:
					dbType = DbType.Binary;
					break;
				case SqlTypeCode.DateTime:
					dbType = DbType.DateTime;
					break;
				case SqlTypeCode.Date:
					dbType = DbType.Date;
					break;
				case SqlTypeCode.TimeStamp:
					dbType = DbType.DateTimeOffset;
					break;
				default:
					throw new NotSupportedException(String.Format("The SQL Type '{0}' cannot be converted to DbType.", typeCode));
			}
		}

		private void ResetSqlType() {
			switch (dbType) {
				case DbType.String:
				case DbType.AnsiString:
					typeCode = SqlTypeCode.VarChar;
					break;
				case DbType.StringFixedLength:
				case DbType.AnsiStringFixedLength:
					typeCode = SqlTypeCode.Char;
					break;
				case DbType.Binary:
					typeCode = SqlTypeCode.Binary;
					break;
				case DbType.Boolean:
					typeCode = SqlTypeCode.Boolean;
					break;
				case DbType.Byte:
					typeCode = SqlTypeCode.TinyInt;
					break;
				case DbType.Int16:
					typeCode = SqlTypeCode.SmallInt;
					break;
				case DbType.Int32:
					typeCode = SqlTypeCode.Integer;
					break;
				case DbType.Int64:
					typeCode = SqlTypeCode.BigInt;
					break;
				case DbType.Single:
					typeCode = SqlTypeCode.Float;
					break;
				case DbType.Double:
					typeCode = SqlTypeCode.Double;
					break;
				case DbType.VarNumeric:
				case DbType.Decimal:
				case DbType.Currency:
					typeCode = SqlTypeCode.Numeric;
					break;
				case DbType.Date:
				case DbType.DateTime2:
					typeCode = SqlTypeCode.Date;
					break;
				case DbType.DateTime:
				case DbType.DateTimeOffset:
					typeCode = SqlTypeCode.TimeStamp;
					break;
				case DbType.Time:
					typeCode = SqlTypeCode.Time;
					break;
				case DbType.Object:
				case DbType.Xml:
					typeCode = SqlTypeCode.Type;
					break;
				default:
					throw new NotSupportedException(String.Format("The DbType '{0}' is not supported by DeveelDB engine", dbType));
			}
		}

		private DbType DiscoverDbType(object value) {
			if (value is bool)
				return DbType.Boolean;
			if (value is byte)
				return DbType.Byte;
			if (value is int)
				return DbType.Int32;
			if (value is short)
				return DbType.Int16;
			if (value is long)
				return DbType.Int64;
			if (value is double)
				return DbType.Double;
			if (value is float)
				return DbType.Single;
			if (value is string)
				return DbType.String;
			if (value is DateTime)
				return DbType.DateTime2;
			if (value is DateTimeOffset)
				return DbType.DateTimeOffset;
			if (value is byte[])
				return DbType.Binary;

			throw new NotSupportedException();
		}

		public override DbType DbType {
			get { return dbType; }
			set {
				dbType = value;
				ResetSqlType();
			}
		}

		public SqlTypeCode SqlType {
			get { return typeCode; }
			set {
				typeCode = value;
				ResetDbType();
			}
		}

		public override ParameterDirection Direction { get; set; }

		public override bool IsNullable { get; set; }

		public override string ParameterName { get; set; }

		public override string SourceColumn { get; set; }

		public override DataRowVersion SourceVersion { get; set; }

		public override object Value { get; set; }

		public override bool SourceColumnNullMapping { get; set; }

		public override int Size { get; set; }

		byte IDbDataParameter.Precision {
			get { return Precision; }
			set { Precision = value; }
		}

		byte IDbDataParameter.Scale {
			get { return Scale; }
			set { Scale = value; }
		}

		public byte Precision { get; set; }

		public byte Scale { get; set; }

		public string Locale { get; set; }

		internal SqlType GetSqlType() {
			switch (SqlType) {
				case SqlTypeCode.Boolean:
				case SqlTypeCode.Bit:
					return PrimitiveTypes.Boolean(SqlType);
				case SqlTypeCode.TinyInt:
				case SqlTypeCode.SmallInt:
				case SqlTypeCode.Integer:
				case SqlTypeCode.BigInt:
					return PrimitiveTypes.Numeric(SqlType);
				case SqlTypeCode.Real:
				case SqlTypeCode.Float:
				case SqlTypeCode.Decimal:
				case SqlTypeCode.Double:
				case SqlTypeCode.Numeric:
					return PrimitiveTypes.Numeric(SqlType, Precision, Scale);
				case SqlTypeCode.String:
				case SqlTypeCode.Char:
				case SqlTypeCode.VarChar:
					return PrimitiveTypes.String(SqlType, Size);
				case SqlTypeCode.Binary:
					case SqlTypeCode.VarBinary:
					return PrimitiveTypes.Binary(SqlType, Size);
				case SqlTypeCode.Time:
				case SqlTypeCode.Date:
				case SqlTypeCode.DateTime:
				case SqlTypeCode.TimeStamp:
					return PrimitiveTypes.DateTime(SqlType);
				case SqlTypeCode.DayToSecond:
				case SqlTypeCode.YearToMonth:
					return PrimitiveTypes.Interval(SqlType);
				case SqlTypeCode.Clob:
				case SqlTypeCode.LongVarChar:
					return PrimitiveTypes.Clob(Size);
				case SqlTypeCode.Blob:
				case SqlTypeCode.LongVarBinary:
					return PrimitiveTypes.Blob(Size);
				default:
					throw new NotSupportedException();
			}
		}
	}
}
