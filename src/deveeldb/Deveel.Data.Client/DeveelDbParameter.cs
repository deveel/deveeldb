// 
//  Copyright 2010-2015 Deveel
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

using Deveel.Data.Types;

namespace Deveel.Data.Client {
	public sealed class DeveelDbParameter : DbParameter, IDbDataParameter {
		private SqlTypeCode typeCode;
		private DbType dbType;

		public override void ResetDbType() {
			if (typeCode == SqlTypeCode.Bit ||
			    typeCode == SqlTypeCode.Boolean) {
				dbType = DbType.Boolean;
			} else if (typeCode == SqlTypeCode.TinyInt) {
				dbType = DbType.Byte;
			} else if (typeCode == SqlTypeCode.SmallInt) {
				dbType = DbType.Int16;
			} else if (typeCode == SqlTypeCode.Integer) {
				dbType = DbType.Int32;
			} else if (typeCode == SqlTypeCode.BigInt) {
				dbType = DbType.Int64;
			} else if (typeCode == SqlTypeCode.Real ||
			           typeCode == SqlTypeCode.Float) {
				dbType = DbType.Single;
			} else if (typeCode == SqlTypeCode.Double) {
				dbType = DbType.Double;
			} else if (typeCode == SqlTypeCode.Decimal) {
				dbType = DbType.Decimal;
			} else if (typeCode == SqlTypeCode.Numeric) {
				dbType = DbType.VarNumeric;
			} else {
				throw new NotSupportedException(String.Format("The SQL Type '{0}' cannot be converted to DbType.", typeCode));
			}
		}

		private void ResetSqlType() {
			if (dbType == DbType.String ||
			    dbType == DbType.AnsiString) {
				typeCode = SqlTypeCode.VarChar;
			} else if (dbType == DbType.StringFixedLength ||
			           dbType == DbType.AnsiStringFixedLength) {
				typeCode = SqlTypeCode.Char;
			} else if (dbType == DbType.Binary) {
				typeCode = SqlTypeCode.Binary;
			} else if (dbType == DbType.Boolean) {
				typeCode = SqlTypeCode.Boolean;
			} else if (dbType == DbType.Byte) {
				typeCode = SqlTypeCode.TinyInt;
			} else if (dbType == DbType.Int16) {
				typeCode = SqlTypeCode.SmallInt;
			} else if (dbType == DbType.Int32) {
				typeCode = SqlTypeCode.Integer;
			} else if (dbType == DbType.Int64) {
				typeCode = SqlTypeCode.BigInt;
			} else if (dbType == DbType.Single) {
				typeCode = SqlTypeCode.Float;
			} else if (dbType == DbType.Double) {
				typeCode = SqlTypeCode.Double;
			} else if (dbType == DbType.VarNumeric ||
			           dbType == DbType.Decimal ||
			           dbType == DbType.Currency) {
				typeCode = SqlTypeCode.Numeric;
			} else if (dbType == DbType.Date ||
			           dbType == DbType.DateTime2) {
				typeCode = SqlTypeCode.Date;
			} else if (dbType == DbType.DateTime ||
			           dbType == DbType.DateTimeOffset) {
				typeCode = SqlTypeCode.TimeStamp;
			} else if (dbType == DbType.Time) {
				typeCode = SqlTypeCode.Time;
			} else if (dbType == DbType.Object ||
			           dbType == DbType.Xml) {
				typeCode = SqlTypeCode.Type;
			} else {
				throw new NotSupportedException(String.Format("The DbType '{0}' is not supported by DeveelDB engine", dbType));
			}
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

		public override System.Data.ParameterDirection Direction { get; set; }

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
	}
}
