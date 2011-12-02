// 
//  DeveelDbDataReader.cs
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
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;

namespace Deveel.Data.Client {
	public sealed class DeveelDbDataReader : DbDataReader, IDataRecord {
		internal DeveelDbDataReader(DeveelDbCommand command, ResultSet result) {
			this.command = command;
			this.result = result;
		}

		private DeveelDbCommand command;
		private ResultSet result;

		public DeveelDbCommand Command {
			get { return command; }
		}

		private object GetValue(int index, bool checkNull) {
			if (result.IsClosed)
				throw new InvalidOperationException();

			object value = result.GetRawColumn(index);
			if (checkNull && 
				((value == null || value == DBNull.Value) ||
				(value is INullable && ((INullable)value).IsNull)))
				throw new SqlNullValueException();

			if (value is LargeObjectRef)
				value = command.Connection.Driver.GetLargeObject((LargeObjectRef)value);

			return value;
		}

		public override void Close() {
			if (result != null)
				result.Close();
			command.OnReaderClosed();
		}

		public override DataTable GetSchemaTable() {
			if (FieldCount == 0)
				return null;

			DataTable table = new DataTable("ColumnsInfo");

			table.Columns.Add("Schema", typeof(string));
			table.Columns.Add("Table", typeof(string));
			table.Columns.Add("Name", typeof(string));
			table.Columns.Add("FullName", typeof(string));
			table.Columns.Add("SqlType", typeof(int));
			table.Columns.Add("DbType", typeof(int));
			table.Columns.Add("Type", typeof(string));
			table.Columns.Add("Size", typeof(int));
			table.Columns.Add("Scale", typeof(int));
			table.Columns.Add("IsUnique", typeof(bool));
			table.Columns.Add("IsNotNull", typeof(bool));
			table.Columns.Add("IsQuantifiable", typeof(bool));
			table.Columns.Add("IsNumeric", typeof(bool));
			table.Columns.Add("UniqueGroup", typeof(int));

			for (int i = 0; i < FieldCount; i++) {
				DataRow row = table.NewRow();

				ColumnInfo column = result.GetColumnInfo(i);

				string fullColumnName = column.Name;

				string schemaName = null;
				string tableName = null;
				string columnName = column.Name;
				if (columnName.StartsWith("@f")) {
					// this is a field, so take the table and schema of the field...
					columnName = columnName.Substring(2, columnName.Length - 2);
					fullColumnName = columnName;

					int index = columnName.IndexOf('.');
					schemaName = columnName.Substring(0, index);
					columnName = columnName.Substring(index + 1);

					index = columnName.IndexOf('.');
					tableName = columnName.Substring(0, index);
					columnName = columnName.Substring(index + 1);
				} else if (columnName.StartsWith("@a")) {
					// this is an alias: strip out the leading indicator...
					columnName = columnName.Substring(2, columnName.Length - 2);
					fullColumnName = columnName;
				}

				row["Schema"] = schemaName;
				row["Table"] = tableName;
				row["Name"] = columnName;
				row["FullName"] = fullColumnName;
				row["SqlType"] = column.SqlType;
				row["DbType"] = (int)column.Type;
				Type type = GetSystemType(column.Type);
				row["Type"] = (type.IsPrimitive ? type.FullName : type.AssemblyQualifiedName);
				row["Size"] = column.Size;
				row["Scale"] = column.Scale;
				row["IsUnique"] = column.Unique;
				row["IsQuantifiable"] = column.IsQuantifiable;
				row["IsNumeric"] = column.IsNumericType;
				row["IsNotNull"] = column.NotNull;
				row["UniqueGroup"] = column.UniqueGroup;

				table.Rows.Add(row);
			}

			return table;
		}

		public override bool NextResult() {
			throw new NotSupportedException();	// yet...
		}

		public override bool Read() {
			return result.Next();
		}

		public override int Depth {
			get { return 1; }
		}

		public override bool IsClosed {
			get { return result.IsClosed; }
		}

		public override int RecordsAffected {
			get { return result.IsUpdate ? result.ToInt32() : 0; }
		}

		public override bool GetBoolean(int ordinal) {
			object value = GetValue(ordinal, true);

			if (value is DeveelDbString)
				return Boolean.Parse(((DeveelDbString) value).Value);
			if (value is DeveelDbNumber)
				return (bool) Convert.ChangeType(value, TypeCode.Boolean);
			if (value is DeveelDbBoolean)
				return ((DeveelDbBoolean) value).Value;

			throw new InvalidCastException();
		}

		public bool GetBoolean(string name) {
			return GetBoolean(GetOrdinal(name));
		}

		public override byte GetByte(int ordinal) {
			object value = GetValue(ordinal, true);
			if (value is DeveelDbNumber)
				return (byte)Convert.ChangeType(value, TypeCode.Byte);

			throw new InvalidCastException();
		}

		public byte GetByte(string name) {
			return GetByte(GetOrdinal(name));
		}

		public DeveelDbLob GetLob(int ordinal) {
			object value = GetValue(ordinal, true);
			return (DeveelDbLob) value;
		}

		public DeveelDbLob GetLob(string name) {
			return GetLob(GetOrdinal(name));
		}

		public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) {
			DeveelDbLob lob = GetLob(ordinal);
			byte[] bytes = lob.GetBytes(dataOffset, length);
			length = System.Math.Min(length, bytes.Length);
			Array.Copy(bytes, 0, buffer, bufferOffset, length);
			return bytes.LongLength;
		}

		public long GetBytes(string name, long dataOffset, byte[] buffer, int bufferOffset, int length) {
			return GetBytes(GetOrdinal(name), dataOffset, buffer, bufferOffset, length);
		}

		public byte[] GetBytes(int ordinal) {
			DeveelDbLob b = GetLob(ordinal);
			if (b.Length >= Int32.MaxValue)
				throw new DataException("LOB too large to return as byte[]");
			
			return b.GetBytes(0, (int) b.Length);
		}

		public byte[] GetBytes(string name) {
			return GetBytes(GetOrdinal(name));
		}

		public override char GetChar(int ordinal) {
			object value = GetValue(ordinal, true);
			if (value is DeveelDbString)
				return ((DeveelDbString) value)[0];
			if (value is DeveelDbNumber)
				return (char)Convert.ChangeType(value, TypeCode.Char);

			throw new InvalidCastException();
		}

		public char GetChar(string name) {
			return GetChar(GetOrdinal(name));
		}

		public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) {
			object value = GetValue(ordinal);
			if (value is DeveelDbLob) {
				DeveelDbLob clob = (DeveelDbLob) value;
				string s = clob.GetString(dataOffset, length);
				if (s == null || s.Length == 0)
					return 0;

				Array.Copy(s.ToCharArray(), 0, buffer, bufferOffset, length);
				return length;
			}
			if (value is DeveelDbString) {
				char[] chars = ((DeveelDbString) value).ToCharArray();
				Array.Copy(chars, 0, buffer, bufferOffset, length);
				return chars.LongLength;
			}

			throw new InvalidCastException();
		}

		public long GetChars(string name, long dataOffset, char[] buffer, int bufferOffset, int length) {
			return GetChars(GetOrdinal(name), dataOffset, buffer, bufferOffset, length);
		}

		public char [] GetChars(int ordinal) {
			object value = GetValue(ordinal, true);

			if (value is DeveelDbString)
				return ((DeveelDbString) value).ToCharArray();
			if (value is DeveelDbLob) {
				DeveelDbLob clob = (DeveelDbLob) value;
				string s = clob.GetString(0, (int) clob.Length);
				return s.ToCharArray();
			}

			throw new InvalidCastException();
		}

		public char[] GetChars(string name) {
			return GetChars(GetOrdinal(name));
		}

		public override Guid GetGuid(int ordinal) {
			object value = GetValue(ordinal, true);

			if (value is DeveelDbString)
				return new Guid(((DeveelDbString)value).Value);
			if (value is DeveelDbBinary)
				return new Guid(((DeveelDbBinary)value).Value);

			throw new InvalidCastException();
		}

		public Guid GetGuid(string name) {
			return GetGuid(GetOrdinal(name));
		}

		public override short GetInt16(int ordinal) {
			Math.BigInteger value = GetBigInteger(ordinal);
			return (short) Convert.ChangeType(value, TypeCode.Int16);
		}

		public short GetInt16(string name) {
			return GetInt16(GetOrdinal(name));
		}

		public override int GetInt32(int ordinal) {
			Math.BigInteger value = GetBigInteger(ordinal);
			return (int)Convert.ChangeType(value, TypeCode.Int32);
		}

		public int GetInt32(string name) {
			return GetInt32(GetOrdinal(name));
		}

		public override long GetInt64(int ordinal) {
			Math.BigInteger value = GetBigInteger(ordinal);
			return (long)Convert.ChangeType(value, TypeCode.Int64);
		}

		public long GetInt64(string name) {
			return GetInt64(GetOrdinal(name));
		}

		public Math.BigDecimal GetBigDecimal(int ordinal) {
			object value = GetValue(ordinal, true);
			if (value is DeveelDbNumber)
				return ((DeveelDbNumber) value).ToBigDecimal();

			throw new InvalidCastException();
		}

		public Math.BigDecimal GetBigDecimal(string name) {
			return GetBigDecimal(GetOrdinal(name));
		}

		public Math.BigInteger GetBigInteger(int ordinal) {
			object value = GetValue(ordinal, true);
			if (value is DeveelDbNumber)
				return ((DeveelDbNumber) value).ToBigInteger();

			throw new InvalidCastException();
		}

		public Math.BigInteger GetBigInteger(string name) {
			return GetBigInteger(GetOrdinal(name));
		}

		public override DateTime GetDateTime(int ordinal) {
			object value = GetValue(ordinal, true);

			if (value is DeveelDbDateTime)
				return ((DeveelDbDateTime) value).Value;
			if (value is DeveelDbNumber)
				return new DateTime(((DeveelDbNumber)value).ToInt64());

			throw new InvalidCastException();
		}

		public DateTime GetDateTime(string name) {
			return GetDateTime(GetOrdinal(name));
		}

		public override string GetString(int ordinal) {
			object value = GetValue(ordinal);
			return value.ToString();
		}

		public override object GetValue(int ordinal) {
			object value = GetValue(ordinal, false);
			if ((value == null || value == DBNull.Value) ||
				(value is INullable && ((INullable)value).IsNull))
				return null;

			return value;
		}

		public object GetValue(string name) {
			return GetValue(GetOrdinal(name));
		}

		public override int GetValues(object[] values) {
			int colCount = System.Math.Min(values.Length, result.ColumnCount);
			for (int i = 0; i < colCount; i++)
				values[i] = GetValue(i);
			return colCount;
		}

		public override bool IsDBNull(int ordinal) {
			object value = GetValue(ordinal);
			if (value == null || value == DBNull.Value)
				return true;
			if (value is INullable)
				return (value as INullable).IsNull;
			return false;
		}

		public bool IsDBNull(string name) {
			return IsDBNull(GetOrdinal(name));
		}

		public override int FieldCount {
			get { return result.ColumnCount; }
		}

		public override object this[int ordinal] {
			get { return GetValue(ordinal); }
		}

		public override object this[string name] {
			get { return this[GetOrdinal(name)]; }
		}

		public override bool HasRows {
			get { return result.RowCount > 0; }
		}

		IDataReader IDataRecord.GetData(int i) {
			return GetData(i);
		}

		public override decimal GetDecimal(int ordinal) {
			//TODO: handle it...
			return new decimal(GetDouble(ordinal));
		}

		public decimal GetDecimal(string name) {
			return GetDecimal(GetOrdinal(name));
		}

		public override double GetDouble(int ordinal) {
			Math.BigDecimal num = GetBigDecimal(ordinal);
			return num.ToDouble();
		}

		public double GetDouble(string name) {
			return GetDouble(GetOrdinal(name));
		}

		public override float GetFloat(int ordinal) {
			Math.BigDecimal num = GetBigDecimal(ordinal);
			return num.ToSingle();
		}

		public float GetFloat(string name) {
			return GetFloat(GetOrdinal(name));
		}

		public override string GetName(int ordinal) {
			ColumnInfo columnInfo = result.GetColumnInfo(ordinal);
			return columnInfo.Name;
		}

		public override int GetOrdinal(string name) {
			return result.FindColumnIndex(name);
		}

		public override string GetDataTypeName(int ordinal) {
			ColumnInfo columnInfo = result.GetColumnInfo(ordinal);
			return columnInfo.SqlType.ToString().ToUpper();
		}

		private static Type GetSystemType(DeveelDbType type) {
			switch (type) {
				case DeveelDbType.Binary:
					return typeof(byte[]);
				case DeveelDbType.Boolean:
					return typeof(bool);
				case DeveelDbType.Int4:
					return typeof(int);
				case DeveelDbType.Int8:
					return typeof(long);
				case DeveelDbType.Number:
					return typeof(Math.BigDecimal);
				case DeveelDbType.String:
					return typeof(string);
				case DeveelDbType.Time:
					return typeof(DateTime);
				case DeveelDbType.Interval:
					return typeof(TimeSpan);
				case DeveelDbType.LOB:
					return typeof(System.IO.Stream);
				case DeveelDbType.Null:
				case DeveelDbType.Unknown:
					return typeof(DBNull);
				default:
					throw new InvalidOperationException();
			}
		}

		public override Type GetFieldType(int ordinal) {
			ColumnInfo columnInfo = result.GetColumnInfo(ordinal);
			DeveelDbType type = columnInfo.Type;
			return GetSystemType(type);
		}

		public override IEnumerator GetEnumerator() {
			return new DbEnumerator(this);
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				Close();
				command = null;
				result = null;
			}

			base.Dispose(disposing);
		}
	}
}