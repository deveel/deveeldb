//  
//  DeveelDbParameter.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Data;
using System.Data.Common;

using Deveel.Math;

namespace Deveel.Data.Client {
	public sealed class DeveelDbParameter : DbParameter {
		public DeveelDbParameter() {
		}

		public DeveelDbParameter(SQLTypes sqlType) {
			SqlType = sqlType;
		}

		public DeveelDbParameter(object value) {
			Value = value;
		}

		public DeveelDbParameter(SQLTypes sqlType, int size)
			: this(sqlType) {
			this.size = size;
		}

		public DeveelDbParameter(SQLTypes sqlType, int size, string sourceColumn)
			: this(sqlType, size) {
			this.sourceColumn = sourceColumn;
		}

		private DbType dbType = DbType.Object;
		private SQLTypes sqlType = SQLTypes.NULL;
		private object value = DBNull.Value;
		// marker style is the default
		internal ParameterStyle paramStyle = ParameterStyle.Marker;
		private long size;
		private byte scale;
		private string sourceColumn;
		private DataRowVersion sourceVersion;
		private string name;
		private ReferenceType ref_type = ReferenceType.Binary;

		#region Implementation of IDataParameter

		public override DbType DbType {
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
			get { return ref_type; }
			set { ref_type = value; }
		}

		public override string ParameterName {
			get { return paramStyle == ParameterStyle.Marker ? "?" : name; }
			set {
				if (paramStyle == ParameterStyle.Marker) {
					if (value != null && value != "?")
						throw new ArgumentException();
				} else {
					if (value == null)
						throw new ArgumentNullException("value");
					if (value.Length < 2)
						throw new ArgumentException("The name must be of at least 2 characters and the first must be a @ prefix.");
					if (value[0] != '@')
						throw new ArgumentException("The name of the parameter must idnicate a @ prefix.");

					name = value;
				}
			}
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
				if (sqlType == SQLTypes.NULL) {
					dbType = GetDbType(this.value);
					sqlType = GetSqlType(this.value);
				}
			}
		}

		#endregion

		#region Implementation of IDbDataParameter

		public new byte Precision {
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

		public SQLTypes SqlType {
			get { return sqlType; }
			set {
				sqlType = value;
				dbType = GetDbType(sqlType);
			}
		}
		#endregion

		private static bool IsNumeric(DbType dbType) {
			if (dbType == DbType.Decimal ||
				dbType == DbType.Double ||
				dbType == DbType.Single ||
				dbType == DbType.VarNumeric)
				return true;
			return false;
		}

		private static DbType GetDbType(SQLTypes sqlType) {
			switch (sqlType) {
				case SQLTypes.BIT:
					return DbType.Boolean;
				case SQLTypes.TINYINT:
					return DbType.Byte;
				case SQLTypes.SMALLINT:
					return DbType.Int16;
				case SQLTypes.INTEGER:
					return DbType.Int32;
				case SQLTypes.BIGINT:
					return DbType.Int64;
				case SQLTypes.FLOAT:
					return DbType.Single;
				case SQLTypes.REAL:
				case SQLTypes.DOUBLE:
					return DbType.Double;

				case SQLTypes.TIME:
					return DbType.Time;
				case SQLTypes.TIMESTAMP:
					return DbType.DateTime;
				case SQLTypes.DATE:
					return DbType.Date;

				case SQLTypes.BINARY:
				case SQLTypes.VARBINARY:
				case SQLTypes.LONGVARBINARY:
				case SQLTypes.BLOB:
					return DbType.Binary;

				case SQLTypes.CHAR:
					return DbType.StringFixedLength;
				case SQLTypes.VARCHAR:
				case SQLTypes.LONGVARCHAR:
				case SQLTypes.CLOB:
					return DbType.String;

				case SQLTypes.NULL:
				case SQLTypes.OBJECT:
					return DbType.Object;
				default:
					return DbType.Object;
			}
		}

		private static DbType GetDbType(object value) {
			if (value is StringObject)
				return DbType.String;
			if (value is ByteLongObject)
				return DbType.Binary;
			if (value is BigNumber) {
				BigNumber num = (BigNumber)value;
				if (num.CanBeInt)
					return DbType.Int32;
				if (num.CanBeLong)
					return DbType.Int64;
				return DbType.VarNumeric;
			}
			if (value is TimeSpan)
				return DbType.DateTime;
			if (value is Enum)
				return DbType.Int32;
			if (value is Guid)
				return DbType.String;

			switch (Type.GetTypeCode(value.GetType())) {
				case TypeCode.Boolean:
					return DbType.Boolean;
				case TypeCode.Byte:
					return DbType.Byte;
				case TypeCode.Char:
					return DbType.StringFixedLength;
				case TypeCode.DateTime:
					return DbType.DateTime;
				case TypeCode.Decimal:
					return DbType.Decimal;
				case TypeCode.Double:
					return DbType.Double;
				case TypeCode.Int16:
					return DbType.Int16;
				case TypeCode.Int32:
					return DbType.Int32;
				case TypeCode.Int64:
					return DbType.Int64;
				case TypeCode.Object:
					return DbType.Binary;
				case TypeCode.SByte:
					return DbType.SByte;
				case TypeCode.Single:
					return DbType.Single;
				case TypeCode.String:
					return DbType.String;
				case TypeCode.UInt16:
					return DbType.UInt16;
				case TypeCode.UInt32:
					return DbType.UInt32;
				case TypeCode.UInt64:
					return DbType.UInt64;
			}
			return DbType.Object;
		}

		private static SQLTypes GetSqlType(object value) {
			if (value is TimeSpan)
				return SQLTypes.TIME;
			if (value is Enum)
				return SQLTypes.INTEGER;
			if (value is Guid)
				return SQLTypes.CHAR;

			switch (Type.GetTypeCode(value.GetType())) {
				case TypeCode.Empty:
					throw new SystemException("Invalid data type");

				case TypeCode.Object:
					return SQLTypes.BLOB;
				case TypeCode.DBNull:
					return SQLTypes.NULL;
				case TypeCode.Char:
				case TypeCode.SByte:
				case TypeCode.Boolean:
				case TypeCode.Byte:
					return SQLTypes.TINYINT;
				case TypeCode.Int16:
				case TypeCode.UInt16:
					return SQLTypes.SMALLINT;
				case TypeCode.Int32:
				case TypeCode.UInt32:
					return SQLTypes.INTEGER;
				case TypeCode.Int64:
				case TypeCode.UInt64:
					return SQLTypes.BIGINT;
				case TypeCode.Single:
					return SQLTypes.FLOAT;
				case TypeCode.Double:
					return SQLTypes.DOUBLE;
				case TypeCode.Decimal:
					return SQLTypes.DECIMAL;
				case TypeCode.DateTime:
					return SQLTypes.TIMESTAMP;
				case TypeCode.String:
					return SQLTypes.VARCHAR;
				default:
					throw new SystemException("Value is of unknown data type");
			}
		}
	}
}