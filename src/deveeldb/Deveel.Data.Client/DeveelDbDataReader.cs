using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;

using Deveel.Data.Protocol;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

using SysDataTable = System.Data.DataTable;
using SysDataRow = System.Data.DataRow;

namespace Deveel.Data.Client {
	public sealed class DeveelDbDataReader : DbDataReader {
		private readonly DeveelDbCommand command;
		private readonly CommandBehavior behavior;

		private bool wasRead;

		internal DeveelDbDataReader(DeveelDbCommand command, CommandBehavior behavior) {
			this.command = command;
			this.behavior = behavior;
		}

		private bool CloseConnection {
			get { return (behavior & CommandBehavior.CloseConnection) != 0; }
		}

		private bool IsSequential {
			get { return (behavior & CommandBehavior.SequentialAccess) != 0; }
		}

		private T GetFinalValue<T>(int ordinal) where T : IConvertible {
			var value = command.CurrentResult.GetRuntimeValue(ordinal);
			if (value == null || value == DBNull.Value)
				return default(T);

			if (value is T)
				return (T)value;

			if (!(value is IConvertible))
				throw new InvalidCastException(String.Format("Cannot convert '{0}' to type '{1}'.", GetName(ordinal), typeof(T)));

			// TODO: Get the culture from the column and use it to convert ...
			return (T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture);
		}

		private ISqlObject GetRawValue(int ordinal) {
			var value = command.CurrentResult.GetRawColumn(ordinal);
			if ((behavior & CommandBehavior.SchemaOnly) != 0)
				return null;

			return value;
		}


		private QueryResultColumn GetColumn(int offset) {
			return command.CurrentResult.GetColumn(offset);
		}

		private int FindColumnIndex(string name) {
			return command.CurrentResult.FindColumnIndex(name);
		}

		public override void Close() {
			try {
				command.CurrentResult.Close();

				if (CloseConnection)
					command.Connection.Close();
			} catch (Exception ex) {
				throw new DeveelDbException("An error occurred while closing the reader", ex);
			} finally {
				if (!CloseConnection)
					command.Connection.EndState();
			}
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				Close();
			}

			base.Dispose(disposing);
		}

		public override DataTable GetSchemaTable() {
			if (FieldCount == 0)
				return null;

			var table = new SysDataTable("ColumnsInfo");

			table.Columns.Add("Schema", typeof (string));
			table.Columns.Add("Table", typeof (string));
			table.Columns.Add("Name", typeof (string));
			table.Columns.Add("FullName", typeof (string));
			table.Columns.Add("SqlType", typeof (int));
			table.Columns.Add("DbType", typeof (string));
			table.Columns.Add("Type", typeof (string));
			table.Columns.Add("Size", typeof (int));
			table.Columns.Add("Scale", typeof (int));
			table.Columns.Add("IsUnique", typeof (bool));
			table.Columns.Add("IsNotNull", typeof (bool));
			table.Columns.Add("IsSizeable", typeof (bool));
			table.Columns.Add("IsNumeric", typeof (bool));
			table.Columns.Add("UniqueGroup", typeof (int));

			for (int i = 0; i < FieldCount; i++) {
				var row = table.NewRow();

				var column = GetColumn(i);

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
				row["SqlType"] = (int) column.Type.SqlType;
				row["DbType"] =  column.Type.Name;
				row["Size"] = column.Size;
				row["Scale"] = column.Scale;
				row["IsUnique"] = column.IsUnique;
				row["IsQuantifiable"] = column.Type is ISizeableType;
				row["IsNumeric"] = column.IsNumericType;
				row["IsNotNull"] = column.IsNotNull;
				row["UniqueGroup"] = column.UniqueGroup;

				table.Rows.Add(row);
			}

			return table;
		}

		public override bool NextResult() {
			return command.NextResult();
		}

		public override bool Read() {
			if (wasRead && ((behavior & CommandBehavior.SingleRow)) != 0)
				return false;

			if (command.CurrentResult.Next()) {
				wasRead = true;
				return true;
			}

			return false;
		}

		public override int Depth {
			get { return 0; }
		}

		public override bool IsClosed {
			get { return command.CurrentResult.Closed; }
		}

		public override int RecordsAffected {
			get {
				if (command.CurrentResult == null ||
					!command.CurrentResult.IsUpdate)
					return -1;

				return command.CurrentResult.AffectedRows;
			}
		}

		public override bool GetBoolean(int ordinal) {
			throw new NotImplementedException();
		}

		public override byte GetByte(int ordinal) {
			throw new NotImplementedException();
		}

		public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) {
			throw new NotImplementedException();
		}

		public override char GetChar(int ordinal) {
			throw new NotImplementedException();
		}

		public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) {
			throw new NotImplementedException();
		}

		public override Guid GetGuid(int ordinal) {
			throw new NotImplementedException();
		}

		public override short GetInt16(int ordinal) {
			throw new NotImplementedException();
		}

		public override int GetInt32(int ordinal) {
			throw new NotImplementedException();
		}

		public override long GetInt64(int ordinal) {
			throw new NotImplementedException();
		}

		public override DateTime GetDateTime(int ordinal) {
			throw new NotImplementedException();
		}

		public override string GetString(int ordinal) {
			throw new NotImplementedException();
		}

		public override object GetValue(int ordinal) {
			throw new NotImplementedException();
		}

		public override int GetValues(object[] values) {
			throw new NotImplementedException();
		}

		public override bool IsDBNull(int ordinal) {
			throw new NotImplementedException();
		}

		public override int FieldCount {
			get { throw new NotImplementedException(); }
		}

		public override object this[int ordinal] {
			get { throw new NotImplementedException(); }
		}

		public override object this[string name] {
			get { throw new NotImplementedException(); }
		}

		public override bool HasRows {
			get { throw new NotImplementedException(); }
		}

		public override decimal GetDecimal(int ordinal) {
			throw new NotImplementedException();
		}

		public override double GetDouble(int ordinal) {
			throw new NotImplementedException();
		}

		public override float GetFloat(int ordinal) {
			throw new NotImplementedException();
		}

		public override string GetName(int ordinal) {
			string columnName = command.CurrentResult.GetColumn(ordinal).Name;
			if (String.IsNullOrEmpty(columnName))
				return String.Empty;
			if (columnName.Length <= 2)
				return columnName;

			if (columnName[0] == '@') {
				if (columnName == "@aresult")
					return String.Empty;

				columnName = columnName.Substring(2);
			}

			return columnName;
		}

		public override int GetOrdinal(string name) {
			throw new NotImplementedException();
		}

		public override string GetDataTypeName(int ordinal) {
			throw new NotImplementedException();
		}

		public override Type GetFieldType(int ordinal) {
			throw new NotImplementedException();
		}

		public override Type GetProviderSpecificFieldType(int ordinal) {
			var column = command.CurrentResult.GetColumn(ordinal);
			if (column == null)
				return null;

			return column.ValueType;
		}

		public override object GetProviderSpecificValue(int ordinal) {
			return GetRawValue(ordinal);
		}

		public override IEnumerator GetEnumerator() {
			return new DbEnumerator(this);
		}
	}
}
