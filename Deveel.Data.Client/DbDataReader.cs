// 
//  DbDataReader.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
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

using Deveel.Math;

namespace Deveel.Data.Client {
	///<summary>
	///</summary>
	public sealed class DbDataReader : IDataReader {
		private readonly DbCommand command;

		private static BigNumber BD_ZERO = BigNumber.fromInt(0);

		internal DbDataReader(DbCommand command) {
			this.command = command;
		}

		#region Implementation of IDisposable

		/// <inheritdoc/>
		public void Dispose() {
            Close();
		}

		#endregion

		#region Implementation of IDataRecord

		/// <inheritdoc/>
		public string GetName(int i) {
			return command.ResultSet.GetColumn(i).Name;
		}

		/// <inheritdoc/>
		public string GetDataTypeName(int i) {
			return command.ResultSet.GetColumn(i).SQLType.ToString().ToUpper();
		}

		/// <inheritdoc/>
		public Type GetFieldType(int i) {
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

		/// <inheritdoc/>
		public object GetValue(int i) {
			Object ob = command.ResultSet.GetRawColumn(i);
			if (ob == null) {
				return ob;
			}
			if (command.Connection.IsStrictGetValue) {
				// Convert depending on the column type,
				ColumnDescription col_desc = command.ResultSet.GetColumn(i);
				SQLTypes sql_type = col_desc.SQLType;

				return command.ObjectCast(ob, sql_type);

			} else {
				// For blobs, return an instance of IBlob.
				if (ob is ByteLongObject ||
					ob is Data.StreamableObject) {
					return command.AsBlob(ob);
				}
			}
			return ob;
		}

		/// <inheritdoc/>
		/// <exception cref="NotImplementedException"/>
		public int GetValues(object[] values) {
			throw new NotImplementedException();
		}

		/// <inheritdoc/>
		public int GetOrdinal(string name) {
			return command.ResultSet.FindColumnIndex(name);
		}

		///<summary>
		///</summary>
		///<param name="i"></param>
		///<returns></returns>
		///<exception cref="DataException"></exception>
		public IBlob GetBlob(int i) {
			// I'm assuming we must return 'null' for a null blob....
			Object ob = command.ResultSet.GetRawColumn(i);
			if (ob != null) {
				try {
					return command.AsBlob(ob);
				} catch (InvalidCastException e) {
					throw new DataException("Column " + i + " is not a binary column.");
				}
			}
			return null;
		}

		///<summary>
		///</summary>
		///<param name="i"></param>
		///<returns></returns>
		///<exception cref="DataException"></exception>
		public IClob GetClob(int i) {
			// I'm assuming we must return 'null' for a null clob....
			Object ob = command.ResultSet.GetRawColumn(i);
			if (ob != null) {
				try {
					return command.AsClob(ob);
				} catch (InvalidCastException) {
					throw new DataException("Column " + i + " is not a character column.");
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
				return BigNumber.fromString(command.MakeString(ob));
			}
		}

		/// <inheritdoc/>
		public bool GetBoolean(int i) {
			Object ob = command.ResultSet.GetRawColumn(i);
			if (ob == null) {
				return false;
			} else if (ob is Boolean) {
				return (Boolean)ob;
			} else if (ob is BigNumber) {
				return ((BigNumber)ob).CompareTo(BD_ZERO) != 0;
			} else if (CanMakeString(ob)) {
				return String.Compare(command.MakeString(ob), "true", true) == 0;
			} else {
				throw new DataException("Unable to cast value in ResultSet to bool");
			}
		}

		/// <inheritdoc/>
		public byte GetByte(int i) {
			// Translates from BigNumber
			BigNumber num = GetBigNumber(i);
			return num == null ? (byte)0 : num.ToByte();
		}

		/// <inheritdoc/>
		/// <exception cref="NotImplementedException"/>
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) {
			throw new NotImplementedException();
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnIndex"></param>
		/// <returns></returns>
		public byte[] GetBytes(int columnIndex) {
			IBlob b = GetBlob(columnIndex);
			if (b == null) {
				return null;
			} else {
				if (b.Length <= Int32.MaxValue) {
					return b.GetBytes(1, (int)b.Length);
				} else {
					throw new DataException("IBlob too large to return as byte[]");
				}
			}
			//    Object ob = GetRawColumn(columnIndex);
			//    if (ob == null) {
			//      return null;
			//    }
			//    else if (ob is ByteLongObject) {
			//      // Return a safe copy of the byte[] array (BLOB).
			//      ByteLongObject b = (ByteLongObject) ob;
			//      byte[] barr = new byte[b.length()];
			//      System.arraycopy(b.ToArray(), 0, barr, 0, b.length());
			//      return barr;
			//    }
			//    else {
			//      throw new DataException("Unable to cast value in ResultSet to byte[]");
			//    }
		}

		/// <inheritdoc/>
		/// <exception cref="NotImplementedException"/>
		public char GetChar(int i) {
			throw new NotImplementedException();
		}

		/// <inheritdoc/>
		/// <exception cref="NotImplementedException"/>
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) {
			throw new NotImplementedException();
		}

		/// <inheritdoc/>
		/// <exception cref="NotImplementedException"/>
		public Guid GetGuid(int i) {
			throw new NotImplementedException();
		}

		/// <inheritdoc/>
		public short GetInt16(int i) {
			// Translates from BigNumber
			BigNumber num = GetBigNumber(i);
			return num == null ? (short)0 : num.ToInt16();
		}

		/// <inheritdoc/>
		public int GetInt32(int i) {
			// Translates from BigNumber
			BigNumber num = GetBigNumber(i);
			return num == null ? 0 : num.ToInt32();
		}

		/// <inheritdoc/>
		public long GetInt64(int i) {
			// Translates from BigNumber
			BigNumber num = GetBigNumber(i);
			return num == null ? 0 : num.ToInt64();
		}

		/// <inheritdoc/>
		public float GetFloat(int i) {
			// Translates from BigNumber
			BigNumber num = GetBigNumber(i);
			return num == null ? 0 : num.ToSingle();
		}

		/// <inheritdoc/>
		public double GetDouble(int i) {
			// Translates from BigNumber
			BigNumber num = GetBigNumber(i);
			return num == null ? 0 : num.ToDouble();
		}

		/// <inheritdoc/>
		public string GetString(int i) {
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
						SQLTypes sql_type = command.ResultSet.GetColumn(i).SQLType;
						return command.ObjectCast(str, sql_type).ToString();
					}
					return str.ToString();
				}
			}
		}

		/// <inheritdoc/>
		public decimal GetDecimal(int i) {
			throw new NotImplementedException();
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
		public DateTime GetDateTime(int i) {
			return (DateTime) command.ResultSet.GetRawColumn(i);
		}

		/// <inheritdoc/>
		/// <exception cref="NotImplementedException"/>
		public IDataReader GetData(int i) {
			throw new NotImplementedException();
		}

		/// <inheritdoc/>
		/// <exception cref="NotImplementedException"/>
		public bool IsDBNull(int i) {
			throw new NotImplementedException();
		}

		/// <inheritdoc/>
		public int FieldCount {
			get { return command.ResultSet.ColumnCount; }
		}

		/// <inheritdoc/>
		public object this[int i] {
			get { return GetValue(i); }
		}

		/// <inheritdoc/>
		public object this[string name] {
			get { return this[GetOrdinal(name)]; }
		}

		#endregion

		#region Implementation of IDataReader

		/// <inheritdoc/>
		public void Close() {
			command.ResultSet.Close();
		}

		System.Data.DataTable IDataReader.GetSchemaTable() {
			throw new NotImplementedException();
		}

		/// <inheritdoc/>
		public bool NextResult() {
			return command.NextResult();
		}

		/// <inheritdoc/>
		public bool Read() {
			return command.ResultSet.Next();
		}

		/// <inheritdoc/>
		public int Depth {
			get { throw new NotImplementedException(); }
		}

		/// <inheritdoc/>
		public bool IsClosed {
			get { return command.ResultSet.closed_on_server; }
		}

		/// <inheritdoc/>
		public int RecordsAffected {
			get { return command.UpdateCount; }
		}

		#endregion
	}
}