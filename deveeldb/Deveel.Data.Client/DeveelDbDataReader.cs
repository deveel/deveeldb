//  
//  DeveelDbDataReader.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
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
using System.Collections;
using System.Data;
using System.Data.Common;

using Deveel.Math;

namespace Deveel.Data.Client {
	///<summary>
	///</summary>
	public sealed class DeveelDbDataReader : DbDataReader {
		private readonly DeveelDbCommand command;
		internal event EventHandler Closed;

		private static readonly BigNumber Zero = 0;

		internal DeveelDbDataReader(DeveelDbCommand command) {
			this.command = command;
		}

		#region Implementation of IDisposable

		/// <inheritdoc/>
		public void Dispose() {
            Close();
		}

		#endregion

		public override bool HasRows {
			get { return command.ResultSet.RowCount > 0; }
		}

		/// <inheritdoc/>
		public override string GetName(int i) {
			return command.ResultSet.GetColumn(i).Name;
		}

		/// <inheritdoc/>
		public override string GetDataTypeName(int i) {
			return command.ResultSet.GetColumn(i).SQLType.ToString().ToUpper();
		}

		/// <inheritdoc/>
		public override Type GetFieldType(int i) {
			return command.ResultSet.GetColumn(i).ObjectType;
		}

		/// <summary>
		/// Returns true if the given object is either an is <see cref="StringObject"/> 
		/// or is an is <see cref="StreamableObject"/>, and therefore can be made into 
		/// a string.
		/// </summary>
		/// <param name="ob"></param>
		/// <returns></returns>
		private static bool CanMakeString(Object ob) {
			return (ob is StringObject || ob is Data.StreamableObject);
		}

		public override IEnumerator GetEnumerator() {
			return new DbEnumerator(this);
		}

		/// <inheritdoc/>
		public override object GetValue(int i) {
			Object ob = command.ResultSet.GetRawColumn(i);
			if (ob == null) {
				return ob;
			}
			if (command.Connection.Settings.StrictGetValue) {
				// Convert depending on the column type,
				ColumnDescription col_desc = command.ResultSet.GetColumn(i);
				SqlType sql_type = col_desc.SQLType;

				return command.ObjectCast(ob, sql_type);

			} else {
				// For blobs, return an instance of IBlob.
				if (ob is ByteLongObject ||
					ob is Data.StreamableObject) {
					// return command.AsBlob(ob);
					return command.GetLob(ob);
				}
			}
			return ob;
		}

		/// <inheritdoc/>
		/// <exception cref="NotImplementedException"/>
		public override int GetValues(object[] values) {
			throw new NotImplementedException();
		}

		/// <inheritdoc/>
		public override int GetOrdinal(string name) {
			return command.ResultSet.FindColumnIndex(name);
		}

		public DeveelDbLob GetLob(int i) {
			// I'm assuming we must return 'null' for a null blob....
			Object ob = command.ResultSet.GetRawColumn(i);
			if (ob != null) {
				try {
					return command.GetLob(ob);
				} catch (InvalidCastException e) {
					throw new DataException("Column " + i + " is not a binary column.");
				}
			}
			return null;
		}

		private BigNumber GetBigNumber(int columnIndex) {
			Object ob = command.ResultSet.GetRawColumn(columnIndex);
			if (ob == null) {
				return null;
			}
			if (ob is BigNumber) {
				return (BigNumber)ob;
			} else {
				return BigNumber.Parse(command.MakeString(ob));
			}
		}

		/// <inheritdoc/>
		public override bool GetBoolean(int i) {
			Object ob = command.ResultSet.GetRawColumn(i);
			if (ob == null) {
				return false;
			} else if (ob is Boolean) {
				return (Boolean)ob;
			} else if (ob is BigNumber) {
				return ((BigNumber)ob).CompareTo(Zero) != 0;
			} else if (CanMakeString(ob)) {
				return String.Compare(command.MakeString(ob), "true", true) == 0;
			} else {
				throw new DataException("Unable to cast value in ResultSet to bool");
			}
		}

		/// <inheritdoc/>
		public override byte GetByte(int i) {
			// Translates from BigNumber
			BigNumber num = GetBigNumber(i);
			return num == null ? (byte)0 : num.ToByte();
		}

		/// <inheritdoc/>
		/// <exception cref="NotImplementedException"/>
		public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) {
			throw new NotImplementedException();
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnIndex"></param>
		/// <returns></returns>
		public byte[] GetBytes(int columnIndex) {
			//IBlob b = GetBlob(columnIndex);
			DeveelDbLob b = GetLob(columnIndex);
			if (b == null) {
				return null;
			} else {
				if (b.Length <= Int32.MaxValue) {
					return b.GetBytes(0, (int)b.Length);
				} else {
					throw new DataException("IBlob too large to return as byte[]");
				}
			}
		}

		/// <inheritdoc/>
		/// <exception cref="NotImplementedException"/>
		public override char GetChar(int i) {
			throw new NotSupportedException();
		}

		/// <inheritdoc/>
		/// <exception cref="NotImplementedException"/>
		public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) {
			// IClob clob = GetClob(i);
			DeveelDbLob clob = GetLob(i);
			if (clob == null)
				return 0;

			string s = clob.GetString(fieldoffset, length);
			int len;
			if (s == null || (len = s.Length) == 0)
				return 0;

			Array.Copy(s.ToCharArray(), 0, buffer, bufferoffset, len);
			return len;
		}

		/// <inheritdoc/>
		public override Guid GetGuid(int i) {
			string s = GetString(i);
			if (s == null || s.Length == 0)
				return Guid.Empty;

			return new Guid(s);
		}

		/// <inheritdoc/>
		public override short GetInt16(int i) {
			// Translates from BigNumber
			BigNumber num = GetBigNumber(i);
			return num == null ? (short)0 : num.ToInt16();
		}

		/// <inheritdoc/>
		public override int GetInt32(int i) {
			// Translates from BigNumber
			BigNumber num = GetBigNumber(i);
			return num == null ? 0 : num.ToInt32();
		}

		/// <inheritdoc/>
		public override long GetInt64(int i) {
			// Translates from BigNumber
			BigNumber num = GetBigNumber(i);
			return num == null ? 0 : num.ToInt64();
		}

		/// <inheritdoc/>
		public override float GetFloat(int i) {
			// Translates from BigNumber
			BigNumber num = GetBigNumber(i);
			return num == null ? 0 : num.ToSingle();
		}

		/// <inheritdoc/>
		public override double GetDouble(int i) {
			// Translates from BigNumber
			BigNumber num = GetBigNumber(i);
			return num == null ? 0 : num.ToDouble();
		}

		/// <inheritdoc/>
		public override string GetString(int i) {
			Object str = command.ResultSet.GetRawColumn(i);
			if (str == null) {
				return null;
			} else {
				if (CanMakeString(str)) {
					return command.MakeString(str);
				} else {
					// For date, time and timestamp we must format as per the JDBC
					// specification.
					if (str is DateTime) {
						SqlType sql_type = command.ResultSet.GetColumn(i).SQLType;
						return command.ObjectCast(str, sql_type).ToString();
					}
					return str.ToString();
				}
			}
		}

		/// <inheritdoc/>
		public override decimal GetDecimal(int i) {
			//TODO: find a better way...
			BigNumber num = GetBigNumber(i);
			return new decimal(num.ToDouble());
		}

		// NOTE: We allow 'GetBigDecimal' methods as extensions to ADO.NET
		//   because they are a key object in our world.

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnIndex"></param>
		/// <returns></returns>
		public BigDecimal GetBigDecimal(int columnIndex) {
			BigNumber bnum = GetBigNumber(columnIndex);
			if (bnum != null) {
				return bnum.ToBigDecimal();
			} else {
				return null;
			}
		}

		/// <inheritdoc/>
		public override DateTime GetDateTime(int i) {
			return (DateTime) command.ResultSet.GetRawColumn(i);
		}

		public new IDataReader GetData(int i) {
			return null;
		}

		/// <inheritdoc/>
		public override bool IsDBNull(int i) {
			Object ob = command.ResultSet.GetRawColumn(i);
			if (ob == null)
				return true;
			if (command.Connection.Settings.StrictGetValue) {
				// Convert depending on the column type,
				ColumnDescription col_desc = command.ResultSet.GetColumn(i);
				SqlType sql_type = col_desc.SQLType;

				ob = command.ObjectCast(ob, sql_type);
				if (ob == null || (ob is TObject && ((TObject)ob).IsNull))
					return true;
			}

			return false;

		}

		/// <inheritdoc/>
		public override int FieldCount {
			get { return command.ResultSet.ColumnCount; }
		}

		/// <inheritdoc/>
		public override object this[int i] {
			get { return GetValue(i); }
		}

		/// <inheritdoc/>
		public override object this[string name] {
			get { return this[GetOrdinal(name)]; }
		}

		/// <inheritdoc/>
		public override void Close() {
			command.ResultSet.Close();
			if (Closed != null)
				Closed(this, EventArgs.Empty);
		}

		public override System.Data.DataTable GetSchemaTable() {
			if (FieldCount == 0)
				return null;

			System.Data.DataTable table = new System.Data.DataTable("ColumnsInfo");

			table.Columns.Add("Schema", typeof (string));
			table.Columns.Add("Table", typeof (string));
			table.Columns.Add("Name", typeof (string));
			table.Columns.Add("FullName", typeof (string));
			table.Columns.Add("SqlType", typeof (int));
			table.Columns.Add("DbType", typeof (int));
			table.Columns.Add("Type", typeof (string));
			table.Columns.Add("Size", typeof (int));
			table.Columns.Add("Scale", typeof (int));
			table.Columns.Add("IsUnique", typeof (bool));
			table.Columns.Add("IsNotNull", typeof (bool));
			table.Columns.Add("IsQuantifiable", typeof (bool));
			table.Columns.Add("IsNumeric", typeof (bool));
			table.Columns.Add("UniqueGroup", typeof (int));

			for (int i = 0; i < FieldCount; i++) {
				System.Data.DataRow row = table.NewRow();

				ColumnDescription column = command.ResultSet.GetColumn(i);

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
				row["SqlType"] = (int)column.SQLType;
				row["DbType"] = (int)column.Type;
				if (column.Type != DbType.Unknown)
					row["Type"] = (column.ObjectType.IsPrimitive ? column.ObjectType.FullName : column.ObjectType.AssemblyQualifiedName);
				else
					row["Type"] = null;
				row["Size"] = column.Size;
				row["Scale"] = column.Scale;
				row["IsUnique"] = column.IsUnique;
				row["IsQuantifiable"] = column.IsQuantifiable;
				row["IsNumeric"] = column.IsNumericType;
				row["IsNotNull"] = column.IsNotNull;
				row["UniqueGroup"] = column.UniqueGroup;

				table.Rows.Add(row);
			}

			return table;
		}

		/// <inheritdoc/>
		public override bool NextResult() {
			return command.NextResult();
		}

		/// <inheritdoc/>
		public override bool Read() {
			return command.ResultSet.Next();
		}

		/// <inheritdoc/>
		public override int Depth {
			get { return 0; }
		}

		//TODO: check this...
		/// <inheritdoc/>
		public override bool IsClosed {
			get { return command.ResultSet.closed_on_server; }
		}

		/// <inheritdoc/>
		public override int RecordsAffected {
			get { return command.UpdateCount; }
		}

		/// <summary>
		/// Gets the instance of the <see cref="DeveelDbCommand"/>
		/// that generated the reader.
		/// </summary>
		public DeveelDbCommand Command {
			get { return command; }
		}
	}
}