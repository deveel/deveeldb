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

namespace Deveel.Data.Client {
	public sealed class DeveelDbParameter : DbParameter, ICloneable {
		public DeveelDbParameter() {
		}

		public DeveelDbParameter(SqlType sqlType) {
			SqlType = sqlType;
		}

		public DeveelDbParameter(object value) {
			Value = value;
		}

		public DeveelDbParameter(SqlType sqlType, int size)
			: this(sqlType) {
			this.size = size;
		}

		public DeveelDbParameter(SqlType sqlType, int size, string sourceColumn)
			: this(sqlType, size) {
			this.sourceColumn = sourceColumn;
		}

		private SysDbType dbType = SysDbType.Object;
		private SqlType sqlType = SqlType.Null;
		private object value = DBNull.Value;
		private long size;
		private byte scale;
		private string sourceColumn;
		private DataRowVersion sourceVersion;
		private string name;
		private ReferenceType refType = ReferenceType.Binary;

		#region Implementation of IDataParameter

		public override SysDbType DbType {
			get { return dbType; }
			set { dbType = value; }
		}

		public override ParameterDirection Direction {
			get { return ParameterDirection.Input; }
			set {
				if (value != ParameterDirection.Input)
					throw new NotSupportedException();
			}
		}

		public override void ResetDbType() {
			dbType = GetDbType(value);
		}

		public override bool SourceColumnNullMapping {
			get { return true; }
			set { }
		}

		//TODO: check...
		public override bool IsNullable {
			get { return true;}
			set { }
		}

		public ReferenceType ReferenceType {
			get { return refType; }
			set { refType = value; }
		}

		public override string ParameterName {
			get { return name; }
			set { name = value; }
		}

		public override string SourceColumn {
			get { return sourceColumn; }
			set { sourceColumn = value; }
		}

		public override DataRowVersion SourceVersion {
			get { return sourceVersion; }
			set { sourceVersion = value; }
		}

		public override object Value {
			get { return value; }
			set {
				this.value = value;
				if (sqlType == SqlType.Null) {
					dbType = GetDbType(this.value);
					sqlType = GetSqlType(this.value);
				}
			}
		}

		#endregion

		#region Implementation of IDbDataParameter

		public byte Precision {
			get { return 0; }
			set {
				if (value != 0)
					throw new ArgumentException();
			}
		}

		public byte Scale {
			get { return scale; }
			set {
				if (!IsNumeric(dbType))
					throw new ArgumentException("Cannot set the scale of a non-numeric paramter.");
				scale = value;
			}
		}

		public override int Size {
			get { return (int)size; }
			set { size = value; }
		}

		public long LongSize {
			get { return size; }
			set { size = value; }
		}

		public SqlType SqlType {
			get { return sqlType; }
			set {
				sqlType = value;
				dbType = GetDbType(sqlType);
			}
		}
		#endregion

		private static bool IsNumeric(SysDbType dbType) {
			if (dbType == SysDbType.Decimal ||
				dbType == SysDbType.Double ||
				dbType == SysDbType.Single ||
				dbType == SysDbType.VarNumeric)
				return true;
			return false;
		}

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

		public object Clone() {
			object paramValue = value;
			if (paramValue is ICloneable)
				paramValue = ((ICloneable) paramValue).Clone();

			var parameter = new DeveelDbParameter {
				value = paramValue,
				sqlType = sqlType,
				sourceColumn = sourceColumn,
				dbType = dbType,
				name = name,
				sourceVersion = sourceVersion,
				size = size,
				scale = scale,
				refType = refType
			};

			return parameter;
		}
	}
}