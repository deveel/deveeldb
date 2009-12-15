//  
//  TypeMemberMapping.cs
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
using System.IO;

using Deveel.Data.Text;

namespace Deveel.Data.Mapping {
	public abstract class TypeMemberMapping {
		internal TypeMemberMapping(TypeMapping dtype, string name) {
			this.name = name;
			this.dtype = dtype;
		}

		private readonly TypeMapping dtype;
		private readonly string name;

		public string Name {
			get { return name; }
		}

		public TypeMapping DeclaringType {
			get { return dtype; }
		}

		internal abstract MemberTypes MemberType { get; }

		internal static SqlType GetSqlTypeFromType(Type memberType) {
			// the numeric types...
			if (memberType == typeof(bool))
				return SqlType.Boolean;
			if (memberType == typeof(byte))
				return SqlType.TinyInt;
			if (memberType == typeof(short))
				return SqlType.SmallInt;
			if (memberType == typeof(int))
				return SqlType.Integer;
			if (memberType == typeof(long))
				return SqlType.BigInt;
			if (memberType == typeof(float))
				return SqlType.Real;
			if (memberType == typeof(double))
				return SqlType.Double;
			if (memberType == typeof(decimal))
				return SqlType.Decimal;

			// the string types
			if (memberType == typeof(char))
				//TODO: support an output size of 1?
				return SqlType.Char;
			if (memberType == typeof(string))
				return SqlType.VarChar;

			if (memberType == typeof(DateTime))
				return SqlType.TimeStamp;
			if (memberType == typeof(TimeSpan))
				return SqlType.DayToSecond;
			if (memberType == typeof(Interval))
				return SqlType.Interval;

			if (memberType == typeof(Stream))
				return SqlType.Blob;

			return SqlType.Unknown;
		}

		internal static TType GetTType(SqlType sqlType, int size, int scale) {
			switch (sqlType) {
				case SqlType.Bit:
				case SqlType.Boolean:
					return TType.GetBooleanType(sqlType);
				case SqlType.TinyInt:
				case SqlType.SmallInt:
				case SqlType.Integer:
				case SqlType.BigInt:
				case SqlType.Numeric:
				case SqlType.Double:
				case SqlType.Real:
					return TType.GetNumericType(sqlType, size, scale);
				case SqlType.Char:
				case SqlType.VarChar:
					return TType.GetStringType(sqlType, size, null, CollationStrength.None, CollationDecomposition.None);
				case SqlType.Date:
				case SqlType.Time:
				case SqlType.TimeStamp:
					return TType.GetDateType(sqlType);
				case SqlType.Interval:
				case SqlType.YearToMonth:
				case SqlType.DayToSecond:
					return TType.GetIntervalType(sqlType);
				default:
					throw new ArgumentException();
			}
		}
	}
}