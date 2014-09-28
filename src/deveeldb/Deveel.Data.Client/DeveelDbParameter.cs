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
using System.Data;
using System.Data.Common;

using Deveel.Data.Sql;

using SysDbType = System.Data.DbType;
using SysParameterDirection = System.Data.ParameterDirection;

namespace Deveel.Data.Client {
	public sealed class DeveelDbParameter : DbParameter {
		private object value;

		public DeveelDbParameter(string name) 
			: this(name, SqlType.Unknown) {
		}

		public DeveelDbParameter(string name, SqlType sqlType) 
			: this(name, sqlType, null) {
		}

		public DeveelDbParameter(string name, SqlType sqlType, object value) 
			: this(name, sqlType, 0, value) {
		}

		public DeveelDbParameter(string name, SqlType sqlType, int size, object value) 
			: this(name, sqlType, size, 0, value) {
		}

		public DeveelDbParameter(string name, SqlType sqlType, int size, byte precision, object value) 
			: this(name, sqlType, size, precision, 0, value) {
		}

		public DeveelDbParameter(string name, SqlType sqlType, int size, byte precision, byte scale, object value) 
			: this(name, sqlType, size, precision, scale, null, value) {
		}

		public DeveelDbParameter(string name, SqlType sqlType, int size, byte precision, byte scale, string sourceColumn, object value) 
			: this(name, sqlType, size, precision, scale, sourceColumn, DataRowVersion.Default, value) {
		}

		public DeveelDbParameter(string name, SqlType sqlType, int size, byte precision, byte scale,
			DataRowVersion rowVersion, object value) : this(name, sqlType, size, precision, scale, null, rowVersion, value) {
		}

		public DeveelDbParameter(string name, SqlType sqlType, int size) 
			: this(name, sqlType, size, 0) {
		}

		public DeveelDbParameter(string name, SqlType sqlType, int size, byte precision) 
			: this(name, sqlType, size, precision, 0) {
		}

		public DeveelDbParameter(string name, SqlType sqlType, int size, byte precision, byte scale) 
			: this(name, sqlType, size, precision, scale, null) {
		}

		public DeveelDbParameter(string name, SqlType sqlType, int size, byte precision, byte scale, string sourceColumn) 
			: this(name, sqlType, size, precision, scale, sourceColumn, DataRowVersion.Default) {
		}

		public DeveelDbParameter(string name, SqlType sqlType, int size, byte precision, byte scale, string sourceColumn,
			DataRowVersion rowVersion) 
			: this(name, sqlType, size, precision, scale, sourceColumn, rowVersion, null) {
		}

		public DeveelDbParameter() 
			: this(SqlType.Unknown) {
		}

		public DeveelDbParameter(SqlType sqlType) 
			: this(sqlType, 0) {
		}

		public DeveelDbParameter(SqlType sqlType, int size) 
			: this(sqlType, size, null) {
		}

		public DeveelDbParameter(object value) 
			: this(SqlType.Unknown, value) {
		}

		public DeveelDbParameter(SqlType sqlType, object value) 
			: this(sqlType, 0, value) {
		}

		public DeveelDbParameter(SqlType sqlType, int size, object value) 
			: this(sqlType, size, 0, value) {
		}

		public DeveelDbParameter(SqlType sqlType, int size, byte precision, object value) 
			: this(sqlType, size, precision, 0, value) {
		}

		public DeveelDbParameter(SqlType sqlType, int size, byte precision, byte scale, object value) 
			: this(sqlType, size, precision, scale, null, value) {
		}

		public DeveelDbParameter(SqlType sqlType, int size, byte precision, byte scale, string sourceColumn, object value) 
			: this(sqlType, size, precision, scale, sourceColumn, DataRowVersion.Default, value) {
		}

		public DeveelDbParameter(SqlType sqlType, int size, byte precision, byte scale, string sourceColumn,
			DataRowVersion rowVersion, object value) 
			: this(null, sqlType, size, precision, scale, sourceColumn, rowVersion, value) {
		}

		public DeveelDbParameter(int size, object value) 
			: this(null, size, value) {
		}

		public DeveelDbParameter(string name, object value) 
			: this(name, 0, value) {
		}

		public DeveelDbParameter(string name, int size, object value) 
			: this(name, size, 0, value) {
		}

		public DeveelDbParameter(int size, byte precision, object value) 
			: this(null, size, precision, value) {
		}

		public DeveelDbParameter(string name, int size, byte precision, object value) 
			: this(name, size, precision, 0, value) {
		}

		public DeveelDbParameter(int size, byte precision, byte scale, object value) 
			: this(null, size, precision, scale, value) {
		}

		public DeveelDbParameter(string name, int size, byte precision, byte scale, object value) 
			: this(name, size, precision, scale, null, value) {
		}

		public DeveelDbParameter(int size, byte precision, byte scale, string sourceColumn, object value) 
			: this(null, size, precision, scale, sourceColumn, value) {
		}

		public DeveelDbParameter(string name, int size, byte precision, byte scale, string sourceColumn, object value) 
			: this(name, size, precision, scale, sourceColumn, DataRowVersion.Default, value) {
		}

		public DeveelDbParameter(int size, byte precision, byte scale, string sourceColumn,
			DataRowVersion rowVersion, object value) 
			: this(null, size, precision, scale, sourceColumn, rowVersion, value) {
		}

		public DeveelDbParameter(string name, int size, byte precision, byte scale, string sourceColumn,
			DataRowVersion rowVersion, object value) 
			: this(name, SqlType.Unknown, size, precision, scale, sourceColumn, rowVersion, value) {
		}

		public DeveelDbParameter(string name, SqlType sqlType, int size, byte precision, byte scale, string sourceColumn,
			DataRowVersion rowVersion, object value) {
			ParameterName = name;
			SqlType = sqlType;
			Size = size;
			Precision = precision;
			Scale = scale;
			SourceColumn = sourceColumn;
			SourceVersion = rowVersion;
			Value = value;
		}

		public override void ResetDbType() {
			DbType = GetDbType(SqlType);
		}

		public SqlType SqlType { get; set; }

		public override System.Data.DbType DbType { get; set; }

		public override SysParameterDirection Direction {
			get { return SysParameterDirection.Input; }
			set {
				if (value != SysParameterDirection.Input)
					throw new NotSupportedException();
			}
		}

		public override bool IsNullable { get; set; }

		public override string ParameterName { get; set; }

		public override string SourceColumn { get; set; }

		public override DataRowVersion SourceVersion { get; set; }

		public override object Value {
			get { return value; }
			set {
				this.value = value;
				if (SqlType == SqlType.Null) {
					DbType = GetDbType(this.value);
					SqlType = GetSqlType(this.value);
				}
			}
		}

		public override bool SourceColumnNullMapping { get; set; }

		public override int Size { get; set; }

		public byte Precision { get; set; }

		public byte Scale { get; set; }

		private static SysDbType GetDbType(SqlType sqlType) {
			switch (sqlType) {
				case SqlType.Bit:
					return SysDbType.Boolean;
				case SqlType.TinyInt:
					return SysDbType.Byte;
				case SqlType.SmallInt:
					return SysDbType.Int16;
				case SqlType.Integer:
					return SysDbType.Int32;
				case SqlType.BigInt:
					return SysDbType.Int64;
				case SqlType.Float:
					return SysDbType.Single;
				case SqlType.Real:
				case SqlType.Double:
					return SysDbType.Double;

				case SqlType.Time:
					return SysDbType.Time;
				case SqlType.TimeStamp:
					return SysDbType.DateTime;
				case SqlType.Date:
					return SysDbType.Date;

				case SqlType.Binary:
				case SqlType.VarBinary:
				case SqlType.LongVarBinary:
				case SqlType.Blob:
					return SysDbType.Binary;

				case SqlType.Char:
					return SysDbType.StringFixedLength;
				case SqlType.VarChar:
				case SqlType.LongVarChar:
				case SqlType.Clob:
					return SysDbType.String;

				case SqlType.Null:
				case SqlType.Object:
					return SysDbType.Object;
				default:
					return SysDbType.Object;
			}
		}

		private static SysDbType GetDbType(object value) {
			if (value is StringObject)
				return SysDbType.String;
			if (value is ByteLongObject)
				return SysDbType.Binary;
			if (value is BigNumber) {
				var num = (BigNumber)value;
				if (num.CanBeInt)
					return SysDbType.Int32;
				if (num.CanBeLong)
					return SysDbType.Int64;
				return SysDbType.VarNumeric;
			}
			if (value is TimeSpan)
				return SysDbType.DateTime;
			if (value is Enum)
				return SysDbType.Int32;
			if (value is Guid)
				return SysDbType.String;

			switch (Type.GetTypeCode(value.GetType())) {
				case TypeCode.Boolean:
					return SysDbType.Boolean;
				case TypeCode.Byte:
					return SysDbType.Byte;
				case TypeCode.Char:
					return SysDbType.StringFixedLength;
				case TypeCode.DateTime:
					return SysDbType.DateTime;
				case TypeCode.Decimal:
					return SysDbType.Decimal;
				case TypeCode.Double:
					return SysDbType.Double;
				case TypeCode.Int16:
					return SysDbType.Int16;
				case TypeCode.Int32:
					return SysDbType.Int32;
				case TypeCode.Int64:
					return SysDbType.Int64;
				case TypeCode.Object:
					return SysDbType.Binary;
				case TypeCode.SByte:
					return SysDbType.SByte;
				case TypeCode.Single:
					return SysDbType.Single;
				case TypeCode.String:
					return SysDbType.String;
				case TypeCode.UInt16:
					return SysDbType.UInt16;
				case TypeCode.UInt32:
					return SysDbType.UInt32;
				case TypeCode.UInt64:
					return SysDbType.UInt64;
			}
			return SysDbType.Object;
		}

		private static SqlType GetSqlType(object value) {
			if (value is TimeSpan)
				return SqlType.Time;
			if (value is Enum)
				return SqlType.Integer;
			if (value is Guid)
				return SqlType.Char;

			switch (Type.GetTypeCode(value.GetType())) {
				case TypeCode.Empty:
					throw new SystemException("Invalid data type");

				case TypeCode.Object:
					return SqlType.Blob;
				case TypeCode.DBNull:
					return SqlType.Null;
				case TypeCode.Char:
				case TypeCode.SByte:
				case TypeCode.Boolean:
				case TypeCode.Byte:
					return SqlType.TinyInt;
				case TypeCode.Int16:
				case TypeCode.UInt16:
					return SqlType.SmallInt;
				case TypeCode.Int32:
				case TypeCode.UInt32:
					return SqlType.Integer;
				case TypeCode.Int64:
				case TypeCode.UInt64:
					return SqlType.BigInt;
				case TypeCode.Single:
					return SqlType.Float;
				case TypeCode.Double:
					return SqlType.Double;
				case TypeCode.Decimal:
					return SqlType.Decimal;
				case TypeCode.DateTime:
					return SqlType.TimeStamp;
				case TypeCode.String:
					return SqlType.VarChar;
				default:
					throw new SystemException("Value is of unknown data type");
			}
		}
	}
}