// 
//  DbParameter.cs
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

using Deveel.Math;

namespace Deveel.Data.Client {
	public sealed class DbParameter : IDbDataParameter {
		internal DbParameter() {
		}

		private object orig_value;
		private DbType dbType = DbType.Object;
		internal object value;
		internal int index;
		private int size;

		#region Implementation of IDataParameter

		public DbType DbType {
			get { return dbType; }
			set { dbType = value; }
		}

		ParameterDirection IDataParameter.Direction {
			get { return ParameterDirection.Input; }
			set { throw new NotSupportedException(); }
		}

		//TODO: check...
		public bool IsNullable {
			get { return true;}
		}

		string IDataParameter.ParameterName {
			get { return "?"; }
			set { throw new NotSupportedException(); }
		}

		string IDataParameter.SourceColumn {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		DataRowVersion IDataParameter.SourceVersion {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		public object Value {
			get { return orig_value; }
			set {
				orig_value = value;
				this.value = CastToDbObject(value);
				dbType = GetDbType(this.value);
			}
		}

		#endregion

		#region Implementation of IDbDataParameter

		byte IDbDataParameter.Precision {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		public byte Scale {
			get {
				if (value == null)
					return 0;
				if (value is BigNumber)
					return (byte) ((BigNumber) value).Scale;
				return 0;
			}
			set {
				if (!IsNumeric(dbType) || !(this.value is BigNumber))
					throw new ArgumentException("Cannot set the scale of a non-numeric paramter.");

				((BigNumber) this.value).SetScale(value, DecimalRoundingMode.HalfEven);
			}
		}

		public int Size {
			get { return size; }
			set { throw new NotImplementedException(); }
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

		private static DbType GetDbType(object value) {
			if (value is StringObject)
				return DbType.String;
			if (value is DateTime)
				return DbType.DateTime;
			if (value is ByteLongObject)
				return DbType.Binary;
			if (value is Boolean)
				return DbType.Boolean;
			if (value is BigNumber) {
				BigNumber num = (BigNumber) value;
				if (num.CanBeInt)
					return DbType.Int32;
				if (num.CanBeLong)
					return DbType.Int64;
				return DbType.VarNumeric;
			}
			return DbType.Object;
		}

		private static bool IsNumber(object value) {
			if (value is sbyte) return true;
			if (value is byte) return true;
			if (value is short) return true;
			if (value is ushort) return true;
			if (value is int) return true;
			if (value is uint) return true;
			if (value is long) return true;
			if (value is ulong) return true;
			if (value is float) return true;
			if (value is double) return true;
			if (value is decimal) return true;
			return false;
		}

		/// <summary>
		/// Converts an <see cref="Object"/> using type data conversion rules.
		/// </summary>
		/// <param name="ob"></param>
		/// <remarks>
		/// For example, <see cref="double"/> is converted to a <c>NUMERIC</c> type 
		/// (<see cref="BigNumber"/>).
		/// </remarks>
		/// <returns></returns>
		static Object CastToDbObject(Object ob) {
			if (ob == null)
				return ob;
			if (ob is String)
				return StringObject.FromString((String)ob);
			if (ob is Boolean)
				return ob;
			if (IsNumber(ob)) {
				if (ob is byte ||
					ob is short ||
					ob is int) {
					return BigNumber.fromInt(Convert.ToInt32(ob));
				} 
				if (ob is long)
					return BigNumber.fromLong(Convert.ToInt64(ob));
				if (ob is float)
					return BigNumber.fromFloat(Convert.ToSingle(ob));
				if (ob is double)
					return BigNumber.fromDouble(Convert.ToDouble(ob));
				
				return BigNumber.fromString(ob.ToString());
			}
			if (ob is byte[])
				return new ByteLongObject((byte[])ob);

			try {
				return ObjectTranslator.Translate(ob);
			} catch (Exception e) {
				// Hacky - we need for ObjectTranslator to throw a better exception
				throw new DataException(e.Message);
			}
		}
	}
}