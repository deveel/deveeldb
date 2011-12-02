// 
//  DeveelDbParameter.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.IO;

namespace Deveel.Data.Client {
	public sealed class DeveelDbParameter : DbParameter, IDbDataParameter, ICloneable {
		public DeveelDbParameter() {
		}

		public DeveelDbParameter(string name, object value) {
			this.name = name;
			Value = value;
		}

		public DeveelDbParameter(object value)
			: this(null, value) {
		}

		public DeveelDbParameter(string name, DeveelDbType type, int size, byte scale) {
			this.name = name;
			this.type = type;
			this.size = size;
			this.scale = scale;
		}

		public DeveelDbParameter(DeveelDbType type, int size, byte scale)
			: this(null, type, size, scale) {
		}

		public DeveelDbParameter(string name, DeveelDbType type, int size)
			: this(name, type, size, 0) {
		}

		public DeveelDbParameter(DeveelDbType type, int size)
			: this(null, type, size) {
		}

		public DeveelDbParameter(string name, DeveelDbType type)
			: this(name, type, -1) {
		}

		public DeveelDbParameter(DeveelDbType type)
			: this(null, type) {
		}

		public DeveelDbParameter(string name, DeveelDbType type, int size, string sourceColumn)
			: this(name, type, size) {
			this.sourceColumn = sourceColumn;
		}

		public DeveelDbParameter(DeveelDbType type, int size, string sourceColumn)
			: this(null, type, size, sourceColumn) {
		}

		public DeveelDbParameter(string name, DeveelDbType type, int size, string sourceColumn, DataRowVersion sourceVersion)
			: this(name, type, size, sourceColumn) {
			this.sourceVersion = sourceVersion;
		}

		public DeveelDbParameter(DeveelDbType type, int size, string sourceColumn, DataRowVersion sourceVersion)
			: this(null, type, size, sourceColumn, sourceVersion) {
		}

		private string name;
		private DeveelDbType type;
		private int size;
		private object value;
		private DbType dbType;
		private bool dbTypeWasSet;
		private string sourceColumn;
		private DataRowVersion sourceVersion = DataRowVersion.Current;
		private bool isNullable;
		private bool sourceColumnNullMapping;
		private byte scale;

		private DeveelDbParameterCollection collection;

		public override void ResetDbType() {
			dbTypeWasSet = false;
		}

		internal DeveelDbCommand Command {
			get { return collection != null ? collection.Command : null; }
		}

		internal DeveelDbParameterCollection Collection {
			get { return collection; }
			set { collection = value; }
		}

		public override DbType DbType {
			get {
				if (!dbTypeWasSet) {
					dbType = GetDbType(type);
					dbTypeWasSet = true;
				}
				return dbType;
			}
			set {
				if (dbType != value)
					SetDeveelDbType(value);

				dbType = value;
				dbTypeWasSet = true;
			}
		}

		public override ParameterDirection Direction {
			get { return ParameterDirection.Input; }
			set {
				if (value != ParameterDirection.Input)
					throw new NotSupportedException();	// yet...
			}
		}

		public override bool IsNullable {
			get { return isNullable; }
			set { isNullable = value; }
		}

		public override string ParameterName {
			get { return name; }
			set {
				if (value != name && collection != null)
					collection.OnParameterNameChanged(name, value);

				name = value;
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
				if ((value == null || value == DBNull.Value) && 
					!isNullable)
					throw new ArgumentException();

				this.value = value;
				AdjustValue();
				if (this.value is ISizeable && !IsNull)
					size = (this.value as ISizeable).Size;
				if (this.value is DeveelDbNumber)
					scale = (byte) ((DeveelDbNumber)this.value).Scale;
				
				SetTypeFromValue();
			}
		}

		public override bool SourceColumnNullMapping {
			get { return sourceColumnNullMapping; }
			set { sourceColumnNullMapping = value; }
		}

		public override int Size {
			get { return size; }
			set { size = value; }
		}

		public byte Scale {
			get { return scale; }
			set { scale = value; }
		}

		public bool IsNull {
			get {
				return (value == null || value == DBNull.Value) ||
				       (value is INullable && ((INullable) value).IsNull);
			}
		}

		private void AdjustValue() {
			if (value == null || value == DBNull.Value)
				return;

			if (value is string)
				value = new DeveelDbString((string) value);
			if (value is char[])
				value = new DeveelDbString(new string((char[]) value));
			if (value is Guid)
				value = new DeveelDbString(((Guid)value).ToString("G"));
			if (value is byte[])
				value = new DeveelDbBinary((byte[]) value);
			if (value is DateTime)
				value = new DeveelDbDateTime((DateTime) value);
			if (value is TimeSpan)
				value = new DeveelDbTimeSpan((TimeSpan) value);
			if (value is int || value is byte || value is short)
				value = new DeveelDbNumber((int) value);
			if (value is long)
				value = new DeveelDbNumber((long) value);
			if (value is float || value is double)
				value = new DeveelDbNumber((double) value);
			if (value is Math.BigDecimal)
				value = new DeveelDbNumber((Math.BigDecimal) value);
			if (value is bool)
				value = new DeveelDbBoolean((bool) value);
			if (value is Stream) {
				DeveelDbCommand command = Command;
				if (command == null)
					throw new InvalidOperationException();

				Stream stream = (Stream) value;
				value = new DeveelDbLob(command.Connection, stream, ReferenceType.Binary, stream.Length);
			}

			if (value is DeveelDbString ||
				value is DeveelDbBoolean ||
				value is DeveelDbBinary ||
				value is DeveelDbDateTime ||
				value is DeveelDbNumber ||
				value is DeveelDbLob)
				return;

			throw new NotSupportedException();
		}

		private void SetTypeFromValue() {
			if (value is DeveelDbNumber) {
				DeveelDbNumber n = (DeveelDbNumber) value;
				if (n.IsFromInt32)
					type = DeveelDbType.Int4;
				else if (n.IsFromInt64)
					type = DeveelDbType.Int8;
				else
					type = DeveelDbType.Number;
			}
			if (value is DeveelDbString)
				type = DeveelDbType.String;
			if (value is DeveelDbBoolean)
				type = DeveelDbType.Boolean;
			if (value is DeveelDbBinary)
				type = DeveelDbType.Binary;
			if (value is DeveelDbLob)
				type = DeveelDbType.LOB;
			if (value is DeveelDbDateTime)
				type = DeveelDbType.Time;
			if (value is DeveelDbTimeSpan)
				type = DeveelDbType.Interval;

			dbType = GetDbType(type);
		}

		private static DbType GetDbType(DeveelDbType type) {
			switch(type) {
					//TODO: LOB and Binary are not the same thing, but DbType
					//      doesn't make a real distinction...
				case DeveelDbType.Binary:
					return DbType.Binary;
				case DeveelDbType.Int4:
					return DbType.Int32;
				case DeveelDbType.Int8:
					return DbType.Int64;
				case DeveelDbType.Number:
					return DbType.VarNumeric;
				case DeveelDbType.String:
					return DbType.StringFixedLength;
				case DeveelDbType.Time:
					return DbType.DateTime;
				case DeveelDbType.Interval:
					return DbType.DateTimeOffset;
				case DeveelDbType.Boolean:
					return DbType.Boolean;
				case DeveelDbType.UDT:
					return DbType.Object;
				default:
					throw new NotSupportedException();
			}
		}

		private void SetDeveelDbType(DbType newDbType) {
			DeveelDbType oldType = type;

			switch(newDbType) {
				case DbType.Boolean:
					type = DeveelDbType.Boolean;
					break;
				case DbType.Byte:
				case DbType.Int16:
				case DbType.Int32:
					type = DeveelDbType.Int4;
					break;
				case DbType.Int64:
					type = DeveelDbType.Int8;
					break;
				case DbType.Single:
				case DbType.Double:
				case DbType.VarNumeric:
				case DbType.Currency:
				case DbType.Decimal:
					type = DeveelDbType.Number;
					break;
				case DbType.Binary:
					type = DeveelDbType.Binary;
					break;
				case DbType.AnsiString:
				case DbType.String:
				case DbType.Object:
					type = DeveelDbType.LOB;
					break;
				case DbType.AnsiStringFixedLength:
				case DbType.StringFixedLength:
					type = DeveelDbType.String;
					break;
				default:
					throw new NotSupportedException();
			}

			if (type != oldType && !IsNull) {
				try {
					//TODO: try to cast existing...
				} catch(InvalidCastException) {
					SetDefaultEmptyValue();
				}
			}
		}

		private void SetDefaultEmptyValue() {
			switch(type) {
				case DeveelDbType.Binary:
					value = new DeveelDbBinary(new byte[0], 0, 0);
					break;
				case DeveelDbType.Boolean:
					value = DeveelDbBoolean.False;
					break;
				case DeveelDbType.Int4:
					value = new DeveelDbNumber(0);
					break;
				case DeveelDbType.Int8:
					value = new DeveelDbNumber(0L);
					break;
				case DeveelDbType.Number:
					value = new DeveelDbNumber(Math.BigDecimal.Zero);
					break;
				case DeveelDbType.String:
					value = DeveelDbString.Empty;
					break;
				case DeveelDbType.Time:
					value = DeveelDbDateTime.MinValue;
					break;
				case DeveelDbType.Interval:
					value = DeveelDbTimeSpan.Zero;
					break;
				default:
					throw new NotSupportedException();
			}
		}

		object ICloneable.Clone() {
			DeveelDbParameter parameter = new DeveelDbParameter(name, type, size, scale);
			parameter.sourceVersion = sourceVersion;
			parameter.sourceColumn = sourceColumn;
			parameter.sourceColumnNullMapping = sourceColumnNullMapping;
			parameter.isNullable = isNullable;

			object paramValue = value;
			if (paramValue is ICloneable)
				paramValue = ((ICloneable) value).Clone();

			parameter.value = paramValue;

			return parameter;
		}
	}
}