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
using System.Collections;
using System.Data;
using System.Data.Common;

using Deveel.Data.Protocol;
using Deveel.Data.Sql;
using Deveel.Math;

using SysDataTable = System.Data.DataTable;
using SysDataRow = System.Data.DataRow;

namespace Deveel.Data.Client {
	///<summary>
	///</summary>
	public sealed class DeveelDbDataReader : DbDataReader {
		private readonly DeveelDbCommand command;
		private readonly CommandBehavior behavior;

		private bool wasRead;

		internal event EventHandler Closed;

		private static readonly BigNumber Zero = 0;

		internal DeveelDbDataReader(DeveelDbCommand command, CommandBehavior behavior) {
			this.command = command;
			this.behavior = behavior;
		}

		#region Implementation of IDisposable

		protected override void Dispose(bool disposing) {
			if (disposing) {
				Close();
			}

			base.Dispose(disposing);
		}

		#endregion

		public override bool HasRows {
			get { return command.CurrentResult.RowCount > 0; }
		}

		/// <inheritdoc/>
		public override string GetName(int i) {
			string columnName = command.CurrentResult.GetColumn(i).Name;
			if (String.IsNullOrEmpty(columnName))
				return String.Empty;
			if (columnName.Length <= 2)
				return columnName;
			if (columnName[0] == '@')
				columnName = columnName.Substring(2);
			return columnName;
		}

		/// <inheritdoc/>
		public override string GetDataTypeName(int i) {
			return command.CurrentResult.GetColumn(i).SQLType.ToString().ToUpper();
		}

		/// <inheritdoc/>
		public override Type GetFieldType(int i) {
			return command.CurrentResult.GetColumn(i).ObjectType;
		}

		/// <summary>
		/// Returns true if the given object is either an is <see cref="StringObject"/> 
		/// or is an is <see cref="StreamableObject"/>, and therefore can be made into 
		/// a string.
		/// </summary>
		/// <param name="ob"></param>
		/// <returns></returns>
		private static bool CanMakeString(Object ob) {
			return (ob is StringObject || ob is StreamableObject);
		}

		public override IEnumerator GetEnumerator() {
			return new DbEnumerator(this);
		}

		private object GetRawValue(int i) {
			if ((behavior & CommandBehavior.SchemaOnly) != 0)
				return null;

			return command.CurrentResult.GetRawColumn(i);
		}

		/// <inheritdoc/>
		public override object GetValue(int i) {
			object ob = GetRawValue(i);
			if (ob == null)
				return null;

			if (command.Connection.Settings.StrictGetValue) {
				// Convert depending on the column type,
				ColumnDescription colDesc = command.CurrentResult.GetColumn(i);
				SqlType sqlType = colDesc.SQLType;

				return command.ObjectCast(ob, sqlType);
			}

			// For blobs, return an instance of IBlob.
			if (ob is ByteLongObject ||
			    ob is StreamableObject) {
				// return command.AsBlob(ob);
				return command.GetLob(ob);
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
			return command.CurrentResult.FindColumnIndex(name);
		}

		public DeveelDbLob GetLob(int i) {
			// I'm assuming we must return 'null' for a null blob....
			object ob = GetRawValue(i);

			if (ob != null) {
				try {
					return command.GetLob(ob);
				} catch (InvalidCastException) {
					throw new DataException("Column " + i + " is not a binary column.");
				}
			}
			return null;
		}

		public BigNumber GetBigNumber(int columnIndex) {
			object ob = GetRawValue(columnIndex);

			if (ob == null)
				return null;
			if (ob is BigNumber)
				return (BigNumber) ob;

			return BigNumber.Parse(command.MakeString(ob));
		}

		/// <inheritdoc/>
		public override bool GetBoolean(int i) {
			object ob = GetRawValue(i);
			if (ob == null)
				return false;
			if (ob is Boolean)
				return (Boolean) ob;
			if (ob is BigNumber)
				return ((BigNumber) ob).CompareTo(Zero) != 0;
			if (CanMakeString(ob))
				return String.Compare(command.MakeString(ob), "true", StringComparison.OrdinalIgnoreCase) == 0;

			throw new DataException("Unable to cast value in ResultSet to bool");
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
			if (b == null)
				return null;

			if (b.Length <= Int32.MaxValue)
				return b.GetBytes(0, (int) b.Length);

			throw new DataException("IBlob too large to return as byte[]");
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
			if (string.IsNullOrEmpty(s))
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
			object str = GetRawValue(i);

			if (str == null)
				return null;
			if (CanMakeString(str))
				return command.MakeString(str);

			// For date, time and timestamp we must format as per the JDBC
			// specification.
			if (str is DateTime) {
				SqlType sqlType = command.CurrentResult.GetColumn(i).SQLType;
				return command.ObjectCast(str, sqlType).ToString();
			}

			return str.ToString();
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
			if (bnum != null)
				return bnum.ToBigDecimal();

			return null;
		}

		/// <inheritdoc/>
		public override DateTime GetDateTime(int i) {
			return (DateTime) GetRawValue(i);
		}

		public new IDataReader GetData(int i) {
			return null;
		}

		/// <inheritdoc/>
		public override bool IsDBNull(int i) {
			object ob = GetRawValue(i);
			if (ob == null || ob == DBNull.Value)
				return true;

			if (command.Connection.Settings.StrictGetValue) {
				// Convert depending on the column type,
				ColumnDescription colDesc = command.CurrentResult.GetColumn(i);
				SqlType sqlType = colDesc.SQLType;

				ob = command.ObjectCast(ob, sqlType);
				if (ob == null || (ob is TObject && ((TObject)ob).IsNull))
					return true;
			}

			return false;

		}

		/// <inheritdoc/>
		public override int FieldCount {
			get { return command.CurrentResult.ColumnCount; }
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
			command.CurrentResult.Close();

			if (Closed != null)
				Closed(this, EventArgs.Empty);

			if ((behavior & CommandBehavior.CloseConnection) != 0)
				command.Connection.Close();
		}

		public override SysDataTable GetSchemaTable() {
			if (FieldCount == 0)
				return null;

			SysDataTable table = new SysDataTable("ColumnsInfo");

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
				SysDataRow row = table.NewRow();

				ColumnDescription column = command.CurrentResult.GetColumn(i);

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
			if ((behavior & CommandBehavior.SingleResult) != 0)
				return false;

			return command.NextResult();
		}

		/// <inheritdoc/>
		public override bool Read() {
			if (wasRead && ((behavior & CommandBehavior.SingleRow)) != 0)
				return false;

			if (command.CurrentResult.Next()) {
				wasRead = true;
				return true;
			}

			return false;
		}

		/// <inheritdoc/>
		public override int Depth {
			get { return 0; }
		}

		//TODO: check this...
		/// <inheritdoc/>
		public override bool IsClosed {
			get { return command.CurrentResult.ClosedOnServer; }
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