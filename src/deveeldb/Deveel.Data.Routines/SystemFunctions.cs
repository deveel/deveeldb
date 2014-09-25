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
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Runtime.Remoting.Contexts;
using System.Security.Cryptography;
using System.Text;

using Deveel.Data.DbSystem;
using Deveel.Data.Query;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Text;
using Deveel.Data.Types;
using Deveel.Data.Util;
using Deveel.Math;

namespace Deveel.Data.Routines {
	public static class SystemFunctions {
		private static FunctionFactory factory;

		public static FunctionFactory Factory {
			get {
				if (factory == null) {
					factory = new SystemFunctionsFactory();
					factory.Init();
				}

				return factory;
			}
		}

		/// <summary>
		/// The number of radians for one degree.
		/// </summary>
		private const double Degree = 0.0174532925;

		/// <summary>
		/// The number of degrees for one radiant.
		/// </summary>
		private const double Radiant = 57.2957795;

		#region ForeignRuleConvert

		internal static TObject PrivilegeString(TObject ob) {
			int privBit = ((BigNumber) ob.Object).ToInt32();
			Privileges privs = new Privileges();
			privs = privs.Add(privBit);
			return TObject.CreateString(privs.ToString());
		}

		internal static TObject FRuleConvert(TObject obj) {
			if (obj.TType is TStringType) {
				String str = null;
				if (!obj.IsNull) {
					str = obj.Object.ToString();
				}
				int v;
				if (str == null || str.Equals("") || str.Equals("NO ACTION")) {
					v = ImportedKey.NoAction;
				} else if (str.Equals("CASCADE")) {
					v = ImportedKey.Cascade;
				} else if (str.Equals("SET NULL")) {
					v = ImportedKey.SetNull;
				} else if (str.Equals("SET DEFAULT")) {
					v = ImportedKey.SetDefault;
				} else if (str.Equals("RESTRICT")) {
					v = ImportedKey.Restrict;
				} else {
					throw new ApplicationException("Unrecognised foreign key rule: " + str);
				}

				// Return the correct enumeration
				return TObject.CreateInt4(v);
			}
			if (obj.TType is TNumericType) {
				var code = obj.ToBigNumber().ToInt32();
				string v;
				if (code == (int) ConstraintAction.Cascade) {
					v = "CASCADE";
				} else if (code == (int) ConstraintAction.NoAction) {
					v = "NO ACTION";
				} else if (code == (int) ConstraintAction.SetDefault) {
					v = "SET DEFAULT";
				} else if (code == (int) ConstraintAction.SetNull) {
					v = "SET NULL";
				} else {
					throw new ApplicationException("Unrecognised foreign key rule: " + code);
				}

				return TObject.CreateString(v);
			}

			throw new ApplicationException("Unsupported type in function argument");
		}

		internal static TObject Instantiate(IQueryContext context, TObject ob, TObject[] args) {
			throw new NotImplementedException();
		}

		internal static TObject SqlTypeString(TObject typeString, TObject typeSize, TObject typeScale) {
			var sb = new StringBuilder();
			sb.Append(typeString.ToStringValue());

			long size = -1;
			long scale = -1;
			if (!typeSize.IsNull) {
				size = typeSize.ToBigNumber().ToInt64();
			}
			if (!typeScale.IsNull) {
				scale = typeScale.ToBigNumber().ToInt64();
			}

			if (size != -1) {
				sb.Append('(');
				sb.Append(size);
				if (scale != -1) {
					sb.Append(',');
					sb.Append(scale);
				}
				sb.Append(')');
			}

			return TObject.CreateString(sb.ToString());
		}

		#endregion

		#region Sequence Functions

		public static TObject NextVal(IQueryContext context, TObject ob) {
			var str = ob.ToStringValue();
			long v = context.NextSequenceValue(str);
			return TObject.CreateInt8(v);
		}

		public static TObject UniqueKey(IQueryContext context, TObject tableName) {
			var str = tableName.Object.ToString();
			long v = context.NextSequenceValue(str);
			return TObject.CreateInt8(v);
		}

		public static TObject CurrVal(IQueryContext context, TObject ob) {
			var str = ob.ToStringValue();
			long v = context.CurrentSequenceValue(str);
			return TObject.CreateInt8(v);
		}

		public static TObject SetVal(IQueryContext context, TObject ob, TObject value) {
			var str = ob.ToStringValue();
			BigNumber num = value.ToBigNumber();
			long v = num.ToInt64();
			context.SetSequenceValue(str, v);
			return TObject.CreateInt8(v);
		}

		public static TObject Identity(IQueryContext context, TObject ob) {
			long v;

			try {
				v = context.CurrentSequenceValue(ob.ToStringValue());
			} catch (StatementException) {
				// TODO: should avoid evaluating here ...

				if (context is DatabaseQueryContext) {
					v = ((DatabaseQueryContext) context).Connection.CurrentUniqueId(ob.ToStringValue());
				} else {
					throw new InvalidOperationException();
				}
			}

			if (v == -1)
				throw new InvalidOperationException("Unable to determine the sequence of the table " + ob);

			return TObject.CreateInt8(v);
		}

		#endregion

		#region Aggregate Functions

		public static TObject Or(TObject ob1, TObject ob2) {
			return ob1 != null ? (ob2.IsNull ? ob1 : (!ob1.IsNull ? ob1.Or(ob2) : ob2)) : ob2;
		}

		public static TObject Max(TObject ob1, TObject ob2) {
			// This will find max,
			return ob1 != null ? (ob2.IsNull ? ob1 : (!ob1.IsNull && ob1.CompareToNoNulls(ob2) > 0 ? ob1 : ob2)) : ob2;
		}

		public static TObject Min(TObject ob1, TObject ob2) {
			// This will find min,
			return ob1 != null ? (ob2.IsNull ? ob1 : (!ob1.IsNull && ob1.CompareToNoNulls(ob2) < 0 ? ob1 : ob2)) : ob2;
		}

		public static TObject Sum(TObject ob1, TObject ob2) {
			// This will sum,
			return ob1 != null ? (ob2.IsNull ? ob1 : (!ob1.IsNull ? ob1.Add(ob2) : ob2)) : ob2;
		}

		#endregion

		#region Arithmetic Functions

		private static double ToRadians(double degrees) {
			return degrees*Degree;
		}

		private static double ToDegrees(double radians) {
			return radians*Radiant;
		}

		public static TObject Pi() {
			return TObject.CreateBigNumber(System.Math.PI);
		}

		public static TObject E() {
			return TObject.CreateBigNumber(System.Math.E);
		}

		public static TObject Sqrt(TObject ob) {
			if (ob.IsNull)
				return ob;

			return TObject.CreateBigNumber(ob.ToBigNumber().Sqrt());
		}

		public static TObject Abs(TObject ob) {
			if (ob.IsNull)
				return ob;

			return TObject.CreateBigNumber(ob.ToBigNumber().Abs());
		}

		public static TObject Tan(TObject ob) {
			if (ob.IsNull)
				return ob;

			double num;
			try {
				num = ob.ToBigNumber().ToDouble();
			} catch (Exception) {
				throw new InvalidOperationException("Unable to cast the argument '" + ob + "' to a double precision number.");
			}

			return TObject.CreateDouble(System.Math.Tan(num));
		}

		public static TObject ATan(TObject ob) {
			if (ob.IsNull)
				return ob;

			var num = ob.ToBigNumber().ToDouble();
			return TObject.CreateBigNumber(System.Math.Atan(num));
		}

		public static TObject Arc(TObject ob) {
			if (ob.IsNull)
				return ob;

			double degrees = ob.ToBigNumber().ToDouble();
			double radians = ToRadians(degrees);
			radians = System.Math.Tan(System.Math.Atan(radians));
			degrees = ToDegrees(radians);

			return TObject.CreateBigNumber(degrees);
		}

		public static TObject TanH(TObject ob) {
			if (ob.IsNull)
				return ob;

			double num;
			try {
				num = ob.ToBigNumber().ToDouble();
			} catch (Exception) {
				throw new InvalidOperationException("Unable to cast the argument '" + ob + "' to a double precision number.");
			}

			return TObject.CreateDouble(System.Math.Tanh(num));
		}

		public static TObject Mod(TObject ob1, TObject ob2) {
			if (ob1.IsNull)
				return ob1;
			if (ob2.IsNull)
				return new TObject(ob1.TType, null);

			var num1 = ob1.ToBigNumber();
			var num2 = ob2.ToBigNumber();

			return TObject.CreateBigNumber(num1.Modulus(num2));
		}

		public static TObject Pow(TObject ob, TObject exp) {
			if (ob.IsNull)
				return ob;

			var num = ob.ToBigNumber();
			var e = exp.ToBigNumber().ToInt32();

			return TObject.CreateBigNumber(num.Pow(e));
		}

		public static TObject Round(TObject ob, TObject precision) {
			if (ob.IsNull)
				return ob;

			BigNumber v = ob.ToBigNumber();
			int d = precision.IsNull ? 0 : precision.ToBigNumber().ToInt32();

			return TObject.CreateBigNumber(v.SetScale(d, RoundingMode.HalfUp));
		}

		public static TObject Round(TObject ob) {
			return Round(ob, TObject.CreateInt4(0));
		}

		public static TObject Log(TObject ob, TObject newBase) {
			if (ob.IsNull)
				return ob;

			var num = ob.ToBigNumber().ToDouble();
			Double log;
			if (newBase.IsNull) {
				log = System.Math.Log(num);
			} else {
				log = System.Math.Log(num, newBase.ToBigNumber().ToDouble());
			}

			return TObject.CreateDouble(log);
		}

		public static TObject Log(TObject ob) {
			return Log(ob, TObject.Null);
		}

		public static TObject Log10(TObject ob) {
			if (ob.IsNull)
				return ob;

			var num = ob.ToBigNumber().ToDouble();
			Double log = System.Math.Log10(num);
			return TObject.CreateDouble(log);
		}

		public static TObject Sin(TObject ob) {
			if (ob.IsNull)
				return ob;

			var num = ob.ToBigNumber().ToDouble();
			return TObject.CreateDouble(System.Math.Sin(num));
		}

		public static TObject ASin(TObject ob) {
			if (ob.IsNull)
				return ob;

			var num = ob.ToBigNumber().ToDouble();
			return TObject.CreateBigNumber(System.Math.Asin(num));
		}

		public static TObject SinH(TObject ob) {
			if (ob.IsNull)
				return ob;

			var num = ob.ToBigNumber().ToDouble();
			return TObject.CreateDouble(System.Math.Sinh(num));
		}

		public static TObject Cos(TObject ob) {
			if (ob.IsNull)
				return ob;

			var num = ob.ToBigNumber().ToDouble();
			return TObject.CreateDouble(System.Math.Cos(num));
		}

		public static TObject ACos(TObject ob) {
			if (ob.IsNull)
				return ob;

			var num = ob.ToBigNumber().ToDouble();
			return TObject.CreateBigNumber(System.Math.Acos(num));
		}

		public static TObject Cot(TObject ob) {
			if (ob.IsNull)
				return ob;

			double degrees = ob.ToBigNumber().ToDouble();
			double radians = ToRadians(degrees);
			double cotan = 1.0/System.Math.Tan(radians);

			return TObject.CreateBigNumber(cotan);
		}

		public static TObject CosH(TObject ob) {
			if (ob.IsNull)
				return ob;

			var num = ob.ToBigNumber().ToDouble();
			return TObject.CreateDouble(System.Math.Cosh(num));
		}

		public static TObject Radians(TObject ob) {
			if (ob.IsNull)
				return TObject.Null;

			double degrees = ob.ToBigNumber().ToDouble();
			double radians = ToRadians(degrees);

			return TObject.CreateBigNumber(radians);
		}

		public static TObject Degrees(TObject ob) {
			if (ob.IsNull)
				return TObject.Null;

			double radians = ob.ToBigNumber().ToDouble();
			double degrees = ToDegrees(radians);

			return TObject.CreateBigNumber(degrees);
		}

		public static TObject Exp(TObject ob) {
			if (ob.IsNull)
				return ob;

			var num = ob.ToBigNumber().ToDouble();
			return TObject.CreateDouble(System.Math.Exp(num));
		}

		public static TObject Rand(IQueryContext context) {
			return TObject.CreateBigNumber(context.NextRandom());
		}

		public static TObject Ceil(TObject ob) {
			if (ob.IsNull)
				return ob;

			var num = ob.ToBigNumber().ToDouble();
			return TObject.CreateBigNumber(System.Math.Ceiling(num));
		}

		public static TObject Floor(TObject ob) {
			if (ob.IsNull)
				return ob;

			var num = ob.ToBigNumber().ToDouble();
			return TObject.CreateBigNumber(System.Math.Floor(num));
		}

		public static TObject Signum(TObject ob) {
			if (ob.IsNull)
				return ob;

			return TObject.CreateBigNumber(ob.ToBigNumber().Signum());
		}

		#endregion

		#region Date/Time Functions

		private static int ExtractField(string field, TObject obj) {
			DateTime dateTime = DateTime.MinValue;
			Interval interval = Interval.Zero;
			bool fromTs = false;

			if (obj.TType is TDateType) {
				dateTime = obj.ToDateTime();
			} else if (obj.TType is TIntervalType) {
				interval = obj.ToInterval();
				fromTs = true;
			} else {
				obj = obj.CastTo(PrimitiveTypes.Date);
				dateTime = obj.ToDateTime();
			}

			int value;

			if (fromTs) {
				switch (field) {
					case "year":
						value = interval.Days;
						break;
					case "month":
						value = interval.Months;
						break;
					case "day":
						value = interval.Days;
						break;
					case "hour":
						value = interval.Hours;
						break;
					case "minute":
						value = interval.Minutes;
						break;
					case "second":
						value = interval.Seconds;
						break;
					default:
						throw new InvalidOperationException("Field " + field + " not supported in an INTERVAL type.");
				}
			} else {
				switch (field) {
					case "year":
						value = dateTime.Year;
						break;
					case "month":
						value = dateTime.Month;
						break;
					case "day":
						value = dateTime.Day;
						break;
					case "hour":
						value = dateTime.Hour;
						break;
					case "minute":
						value = dateTime.Minute;
						break;
					case "second":
						value = dateTime.Second;
						break;
					case "millis":
						value = dateTime.Millisecond;
						break;
					default:
						throw new InvalidOperationException("Field " + field + " not supported in a TIME type.");
				}
			}

			return value;
		}


		public static TObject Extract(TObject date, TObject field) {
			if (field.IsNull)
				throw new ArgumentException("The first parameter of EXTRACT function can't be NULL.");

			if (date.IsNull)
				return TObject.Null;

			string fieldStr = field.ToStringValue();

			return TObject.CreateInt4(ExtractField(fieldStr, date));
		}

		public static TObject Year(TObject date) {
			return Extract(date, TObject.CreateString("year"));
		}

		public static TObject Month(TObject date) {
			return Extract(date, TObject.CreateString("month"));
		}

		public static TObject Day(TObject date) {
			return Extract(date, TObject.CreateString("day"));
		}

		public static TObject Hour(TObject date) {
			return Extract(date, TObject.CreateString("hour"));
		}

		public static TObject Minute(TObject date) {
			return Extract(date, TObject.CreateString("minute"));
		}

		public static TObject Second(TObject date) {
			return Extract(date, TObject.CreateString("second"));
		}

		public static TObject Millis(TObject date) {
			return Extract(date, TObject.CreateString("millis"));
		}

		public static TObject ToDate(TObject ob) {
			var ttype = TType.GetDateType(SqlType.Date);

			if (ob.IsNull)
				return new TObject(ttype, DateTime.Today);

			var str = ob.ToStringValue();

			DateTime date;
			try {
				date = CastHelper.ToDate(str);
			} catch (FormatException e) {
				throw new ApplicationException(String.Format("Cannot convert string '{0}' into a DATE", str), e);
			}

			return new TObject(ttype, date);
		}

		public static TObject CurrentDate() {
			return ToDate(TObject.Null);
		}

		public static TObject ToTimeStamp(TObject ob) {
			var ttype = TType.GetDateType(SqlType.TimeStamp);

			if (ob.IsNull)
				return new TObject(ttype, DateTime.Today);

			var str = ob.ToStringValue();

			DateTime date;
			try {
				date = CastHelper.ToTimeStamp(str);
			} catch (FormatException e) {
				throw new ApplicationException(String.Format("Cannot convert string '{0}' into a TIMESTAMP", str), e);
			}

			return new TObject(ttype, date);
		}

		public static TObject CurrentTimestamp() {
			return ToTimeStamp(TObject.Null);
		}

		public static TObject ToTime(TObject ob) {
			var ttype = TType.GetDateType(SqlType.Time);

			if (ob.IsNull)
				return new TObject(ttype, DateTime.Now);

			var str = ob.ToStringValue();

			DateTime date;
			try {
				date = CastHelper.ToTime(str);
			} catch (FormatException e) {
				throw new ApplicationException(String.Format("Cannot convert string '{0}' into a TIMESTAMP", str), e);
			}

			return new TObject(ttype, date);
		}

		public static TObject CurrentTime() {
			return ToTime(TObject.Null);
		}

		public static TObject ToInterval(TObject ob) {
			return ToInterval(ob, TObject.CreateString("full"));
		}

		public static TObject ToInterval(TObject ob, TObject fieldOb) {
			if (ob.IsNull)
				return ob;

			if (!(ob.TType is TStringType))
				ob = ob.CastTo(PrimitiveTypes.VarString);

			string s = ob.ToStringValue();

			string field = null;
			if (fieldOb != null && !fieldOb.IsNull)
				field = fieldOb.ToStringValue();

			Interval interval = Interval.Zero;
			if (!string.IsNullOrEmpty(field)) {
				switch (field.ToLower()) {
					case "year":
						interval = new Interval(Int32.Parse(s), 0);
						break;
					case "month":
						interval = new Interval(0, Int32.Parse(s));
						break;
					case "day":
						interval = new Interval(Int32.Parse(s), 0, 0, 0);
						break;
					case "hour":
						interval = new Interval(0, Int32.Parse(s), 0, 0);
						break;
					case "minute":
						interval = new Interval(0, 0, Int32.Parse(s), 0);
						break;
					case "second":
						interval = new Interval(0, 0, 0, Int32.Parse(s));
						break;
					case "day to second":
						interval = Interval.Parse(s, IntervalForm.DayToSecond);
						break;
					case "year to month":
						interval = Interval.Parse(s, IntervalForm.YearToMonth);
						break;
					case "full":
						interval = Interval.Parse(s, IntervalForm.Full);
						break;
					default:
						throw new InvalidOperationException("The conversion to INTERVAL is not supported for " + field + ".");
				}
			} else {
				interval = Interval.Parse(s, IntervalForm.Full);
			}

			return TObject.CreateInterval(interval);
		}

		public static TObject AddMonths(TObject ob, TObject n) {
			if (ob.IsNull || !(ob.TType is TDateType))
				return ob;

			if (n.IsNull)
				return ob;

			DateTime date = ob.ToDateTime();
			int value = n.ToBigNumber().ToInt32();

			return TObject.CreateDateTime(date.AddMonths(value));
		}

		public static TObject MonthsBetween(TObject ob1, TObject ob2) {
			if (ob1.IsNull || ob2.IsNull)
				return TObject.Null;

			DateTime date1 = ob1.ToDateTime();
			DateTime date2 = ob2.ToDateTime();

			var span = new Interval(date1, date2);
			return TObject.CreateInt4(span.Months);
		}

		public static TObject DbTimeZone() {
			return TObject.CreateString(TimeZone.CurrentTimeZone.StandardName);
		}

		public static TObject NextDay(TObject ob, TObject day) {
			if (ob.IsNull)
				return ob;

			DateTime date = ob.ToDateTime();
			DateTime nextDate = GetNextDateForDay(date, GetDayOfWeek(day));

			return TObject.CreateDateTime(nextDate);
		}

		public static TObject LastDay(TObject ob) {
			if (ob.IsNull)
				return ob;

			DateTime date = ob.ToDateTime();

			var evalDate = new DateTime(date.Year, date.Month, 1);
			evalDate = evalDate.AddMonths(1).Subtract(new TimeSpan(1, 0, 0, 0, 0));

			return TObject.CreateDateTime(evalDate);
		}

		private static DayOfWeek GetDayOfWeek(TObject ob) {
			if (ob.TType is TNumericType)
				return (DayOfWeek) ob.ToBigNumber().ToInt32();
			return (DayOfWeek) Enum.Parse(typeof (DayOfWeek), ob.ToStringValue(), true);
		}

		private static DateTime GetNextDateForDay(DateTime startDate, DayOfWeek desiredDay) {
			// Given a date and day of week,
			// find the next date whose day of the week equals the specified day of the week.
			return startDate.AddDays(DaysToAdd(startDate.DayOfWeek, desiredDay));
		}

		private static int DaysToAdd(DayOfWeek current, DayOfWeek desired) {
			// f( c, d ) = g( c, d ) mod 7, g( c, d ) > 7
			//           = g( c, d ), g( c, d ) < = 7
			//   where 0 <= c < 7 and 0 <= d < 7

			int c = (int) current;
			int d = (int) desired;
			int n = (7 - c + d);

			return (n > 7) ? n%7 : n;
		}

		public static TObject DateFormat(TObject date, TObject format) {
			// If expression resolves to 'null' then return null
			if (date.IsNull) {
				return date;
			}

			if (!(date.TType is TDateType))
				throw new Exception("Date to format must be DATE, TIME or TIMESTAMP");

			var d = date.ToDateTime();

			string formatString = format.ToString();

			// TODO: support context culture ...
			return TObject.CreateString(d.ToString(formatString, CultureInfo.InvariantCulture));
		}

		#endregion

		#region StringFunctions

		public static TObject Concat(TObject[] args) {
			var cc = new StringBuilder();

			CultureInfo locale = null;
			CollationStrength strength = 0;
			CollationDecomposition decomposition = 0;
			foreach (TObject ob in args) {
				if (ob.IsNull)
					return ob;

				cc.Append(ob.Object);

				TType type1 = ob.TType;
				if (locale == null && type1 is TStringType) {
					var strType = (TStringType) type1;
					locale = strType.Locale;
					strength = strType.Strength;
					decomposition = strType.Decomposition;
				}
			}

			// We inherit the locale from the first string parameter with a locale,
			// or use a default VarString if no locale found.
			TType type;
			if (locale != null) {
				type = new TStringType(SqlType.VarChar, -1, locale, strength, decomposition);
			} else {
				type = PrimitiveTypes.VarString;
			}

			return new TObject(type, cc.ToString());
		}

		public static TObject Replace(TObject ob1, TObject ob2, TObject ob3) {
			if (ob1.IsNull)
				return ob1;

			if (ob2.IsNull)
				return ob1;

			string s = ob1.ToStringValue();
			string oldValue = ob2.ToStringValue();
			string newValue = ob3.ToStringValue();

			string result = s.Replace(oldValue, newValue);
			return TObject.CreateString(result);
		}

		public static TObject Substring(TObject ob, TObject startOffset) {
			if (ob.IsNull)
				return ob;

			String str = ob.Object.ToString();
			int strLength = str.Length - (startOffset.ToBigNumber().ToInt32()-1);

			return Substring(ob, startOffset, TObject.CreateInt4(strLength));
		}

		public static TObject Substring(TObject ob, TObject startOffset, TObject length) {
			if (ob.IsNull)
				return ob;

			string str = ob.Object.ToString();
			int strLength = str.Length;
			int start = startOffset.ToBigNumber().ToInt32()- 1;
			int len = length.ToBigNumber().ToInt32();

			// Make sure this call is safe for all lengths of string.
			if (start < 0)
				start = 0;

			if (start > strLength)
				return TObject.CreateString("");

			if (len + start > strLength)
				len = (strLength - start);

			if (len < 1)
				return TObject.CreateString("");

			return TObject.CreateString(str.Substring(start, len));
		}

		public static TObject InString(TObject ob, TObject search) {
			return InString(ob, search, TObject.CreateInt4(1));
		}

		public static TObject InString(TObject ob, TObject search, TObject startOffset) {
			if (ob.IsNull)
				return ob;

			string str = ob.Object.ToString();
			int strLength = str.Length;

			return InString(ob, search, startOffset, TObject.CreateBigNumber(1));
		}

		public static TObject InString(TObject ob, TObject search, TObject startOffset, TObject occurrence) {
			if (ob.IsNull)
				return TObject.CreateBigNumber(-1);

			var s = ob.ToStringValue();
			var sub = search.ToStringValue();

			if (String.IsNullOrEmpty(sub))
				return TObject.CreateBigNumber(-1);

			var start = 0;
			var occ = 1;

			if (!startOffset.IsNull)
				start = startOffset.ToBigNumber().ToInt32() - 1;
			if (!occurrence.IsNull)
				occ = occurrence.ToBigNumber().ToInt32();

			if (occ < 1)
				occ = 1;

			if (start < 0)
				start = 0;

			if (String.IsNullOrEmpty(s))
				return TObject.CreateBigNumber(-1);

			int count = -1;
			int lastIndex = -1;
			while (++count < occ) {
				lastIndex = s.IndexOf(sub, start + (lastIndex+1));
			}

			if (lastIndex >= 0)
				lastIndex = lastIndex + 1;

			return TObject.CreateBigNumber(lastIndex);
		}

		public static TObject LPad(TObject ob, TObject totalWidth) {
			return LPad(ob, totalWidth, TObject.CreateString(" "));
		}

		public static TObject LPad(TObject ob, TObject totalWidth, TObject pad) {
			if (ob.IsNull)
				return ob;

			var s = ob.Object.ToString();
			var width = totalWidth.ToBigNumber().ToInt32();

			char c = ' ';
			if (!pad.IsNull) {
				var sPad = pad.ToString();
				if (sPad.Length != 0)
					c = sPad[0];
			}

			//TODO: use StringBuilder based solution to allow long-strings?
			return TObject.CreateString(s.PadLeft(width, c));
		}

		public static TObject RPad(TObject ob, TObject totalWidth) {
			return RPad(ob, totalWidth, TObject.CreateString(" "));
		}

		public static TObject RPad(TObject ob, TObject totalWidth, TObject pad) {
			if (ob.IsNull)
				return ob;

			var s = ob.Object.ToString();
			var width = totalWidth.ToBigNumber().ToInt32();

			char c = ' ';
			if (!pad.IsNull) {
				var sPad = pad.ToString();
				if (sPad.Length != 0)
					c = sPad[0];
			}

			//TODO: use StringBuilder based solution to allow long-strings?
			return TObject.CreateString(s.PadRight(width, c));
		}

		public static TObject CharLength(TObject ob) {
			if (!(ob.TType is TStringType) || ob.IsNull)
				return TObject.Null;

			var s = (IStringAccessor) ob.Object;
			if (s == null)
				return TObject.Null;

			return TObject.CreateInt4(s.Length);
		}

		public static TObject Soundex(TObject ob) {
			if (ob.IsNull)
				return ob;

			return TObject.CreateString(Text.Soundex.Default.Compute(ob.ToStringValue()));
		}

		public static TObject Lower(TObject ob) {
			if (ob.IsNull)
				return ob;

			var locale = ((TStringType) ob.TType).Locale;
			if (locale == null)
				locale = CultureInfo.InvariantCulture;

			return TObject.CreateString(ob.ToStringValue().ToLower(locale));
		}

		public static TObject Upper(TObject ob) {
			if (ob.IsNull)
				return ob;

			var locale = ((TStringType) ob.TType).Locale;
			if (locale == null)
				locale = CultureInfo.InvariantCulture;

			return TObject.CreateString(ob.ToStringValue().ToUpper(locale));
		}

		public static TObject Trim(TObject ob, TObject type, TObject toTrim) {
			// The content to trim.
			if (ob.IsNull)
				return ob;

			// Characters to trim
			if (toTrim.IsNull)
				return toTrim;
			if (type.IsNull)
				return TObject.CreateString((StringObject) null);

			String characters = toTrim.Object.ToString();
			String typeStr = type.Object.ToString();
			String str = ob.Object.ToString();

			int skip = characters.Length;
			// Do the trim,
			if (typeStr.Equals("leading") || typeStr.Equals("both")) {
				// Trim from the start.
				int scan = 0;
				while (scan < str.Length &&
				       str.IndexOf(characters, scan) == scan) {
					scan += skip;
				}
				str = str.Substring(System.Math.Min(scan, str.Length));
			}
			if (typeStr.Equals("trailing") || typeStr.Equals("both")) {
				// Trim from the end.
				int scan = str.Length - 1;
				int i = str.LastIndexOf(characters, scan);
				while (scan >= 0 && i != -1 && i == scan - skip + 1) {
					scan -= skip;
					i = str.LastIndexOf(characters, scan);
				}
				str = str.Substring(0, System.Math.Max(0, scan + 1));
			}

			return TObject.CreateString(str);
		}

		public static TObject LTrim(TObject ob, TObject toTrim) {
			if (ob.IsNull)
				return ob;

			string str = ob.Object.ToString();
			string characters = toTrim.Object.ToString();
			str = str.TrimStart(characters.ToCharArray());

			return TObject.CreateString(str);
		}

		public static TObject LTrim(TObject ob) {
			return LTrim(ob, TObject.CreateString(" "));
		}

		public static TObject RTrim(TObject ob, TObject toTrim) {
			if (ob.IsNull)
				return ob;

			string characters = toTrim.ToStringValue();
			String str = ob.Object.ToString();

			str = str.TrimEnd(characters.ToCharArray());

			return TObject.CreateString(str);
		}

		public static TObject RTrim(TObject ob) {
			return RTrim(ob, TObject.CreateString(" "));
		}

		#endregion

		#region Binary Functions

		private static readonly char[] HexDigits = {
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
			'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
			'u', 'v', 'w', 'x', 'y', 'z'
		};

		public static TObject BinaryToHex(TObject ob) {
			if (ob.IsNull)
				return ob;

			if (!(ob.TType is TBinaryType))
				throw new Exception("'binarytohex' parameter type is not a binary object.");

			var sb = new StringBuilder();
			var blob = (IBlobAccessor) ob.Object;
			Stream bin = blob.GetInputStream();
			try {
				int bval = bin.ReadByte();
				while (bval != -1) {
					//TODO: check if this is correct...
					sb.Append(HexDigits[((bval >> 4) & 0x0F)]);
					sb.Append(HexDigits[(bval & 0x0F)]);
					bval = bin.ReadByte();
				}
			} catch (IOException e) {
				throw new Exception("IO ApplicationException: " + e.Message, e);
			}

			return TObject.CreateString(sb.ToString());
		}

		public static TObject HexToBinary(TObject ob) {
			var str = ob.ToStringValue();

			int strLen = str.Length;
			if (strLen == 0)
				return new TObject(PrimitiveTypes.BinaryType, new ByteLongObject(new byte[0]));

			// We translate the string to a byte array,
			var buf = new byte[(strLen + 1)/2];
			int index = 0;
			if (buf.Length*2 != strLen) {
				buf[0] = Convert.ToByte(Char.GetNumericValue(str[0]));
				++index;
			}

			int v;
			for (int i = index; i < strLen; i += 2) {
				v = ((int) Char.GetNumericValue(str[i]) << 4) |
				    ((int) Char.GetNumericValue(str[i + 1]));
				buf[index] = (byte) (v & 0x0FF);
				++index;
			}

			return new TObject(PrimitiveTypes.BinaryType, new ByteLongObject(buf));
		}

		public static TObject Crc32(TObject ob) {
			//TODO: needs some revision...
			if (!(ob.TType is TBinaryType))
				throw new InvalidOperationException("The function CRC32 accepts only one argument of type BINARY");

			var blob = (IBlobAccessor) ob.Object;
			var stream = new MemoryStream(blob.Length);
			CopyStream(blob.GetInputStream(), stream);

			var crc32Hash = new Crc32Hash();
			crc32Hash.ComputeHash(stream);

			var bytes = BitConverter.GetBytes(crc32Hash.CrcValue);
			return new TObject(PrimitiveTypes.BinaryType, bytes);
		}

		public static TObject Adler32(TObject ob) {
			if (ob.IsNull)
				return TObject.Null;

			//TODO: needs some revision...
			MemoryStream stream;
			if (!(ob.TType is TBinaryType))
				throw new InvalidOperationException("The function ADLER32 accepts only one argument of BINARY type.");

			var blob = (IBlobAccessor) ob.Object;
			stream = new MemoryStream(blob.Length);
			CopyStream(blob.GetInputStream(), stream);

			var adler32 = new Adler32Hash();
			byte[] result = adler32.ComputeHash(stream);
			return new TObject(PrimitiveTypes.BinaryType, result);
		}

		private static void CopyStream(Stream input, Stream output) {
			const int bufferSize = 1024;
			byte[] buffer = new byte[bufferSize];
			int readCount;
			while ((readCount = input.Read(buffer, 0, bufferSize)) != 0) {
				output.Write(buffer, 0, readCount);
			}
		}

		public static TObject Compress(TObject ob) {
			if (ob.IsNull)
				return TObject.Null;

			if (!(ob.TType is TBinaryType))
				throw new InvalidOperationException("The function COMPRESS accepts only one argument of type BINARY");

			var blob = (IBlobAccessor) ob.Object;
			var stream = new MemoryStream(blob.Length);
			CopyStream(blob.GetInputStream(), stream);

			var tempStream = new MemoryStream();
			var outputStream = new DeflateStream(tempStream, CompressionMode.Compress);

			const int bufferSize = 1024;
			byte[] buffer = new byte[bufferSize];

			int bytesRead;
			while ((bytesRead = stream.Read(buffer, 0, bufferSize)) != 0) {
				outputStream.Write(buffer, 0, bytesRead);
			}

			outputStream.Flush();

			byte[] result = tempStream.ToArray();
			return new TObject(PrimitiveTypes.BinaryType, result);
		}

		public static TObject Uncompress(TObject ob) {
			if (ob.IsNull)
				return TObject.Null;

			if (!(ob.TType is TBinaryType))
				throw new InvalidOperationException("Function UNCOMPRESS accepts only one argument of type BINARY");

			var blob = (IBlobAccessor) ob.Object;
			var stream = new MemoryStream(blob.Length);
			CopyStream(blob.GetInputStream(), stream);

			var tmpStream = new MemoryStream();
			var inputStream = new DeflateStream(stream, CompressionMode.Decompress);

			const int bufferSize = 1024;
			byte[] buffer = new byte[bufferSize];

			int bytesRead;
			while ((bytesRead = inputStream.Read(buffer, 0, bufferSize)) != 0) {
				tmpStream.Write(buffer, 0, bytesRead);
			}

			byte[] output = tmpStream.ToArray();
			return new TObject(PrimitiveTypes.BinaryType, output);
		}

		public static TObject OctetLength(TObject ob) {
			if (ob.IsNull)
				return TObject.Null;

			if (!(ob.TType is TBinaryType))
				throw new InvalidOperationException("Function OCTET_LENGTH requires a single argument of type BINARY");

			var b = (IBlobAccessor) ob.Object;
			if (b == null)
				return TObject.Null;

			long size = b.Length;
			if (b is IRef)
				size = (b as IRef).RawSize;

			return TObject.CreateBigNumber(size);
		}

		#endregion

		public static TObject Coalesce(TObject[] args) {
			int count = args.Length;
			for (int i = 0; i < count - 1; ++i) {
				TObject res = args[i];
				if (!res.IsNull) {
					return res;
				}
			}

			return args[count - 1];
		}

		public static TObject NullIf(TObject ob1, TObject ob2) {
			if (ob1.IsNull)
				throw new InvalidOperationException("Cannot compare to a NULL argument.");

			if (!ob1.TType.IsComparableType(ob2.TType))
				throw new InvalidOperationException("The types of the two arguments are not comparable.");

			return ob1.CompareTo(ob2) == 0 ? TObject.Null : ob1;
		}

		public static TObject Greatest(TObject[] args) {
			TObject great = null;
			foreach (TObject ob in args) {
				if (ob.IsNull)
					return ob;

				if (great == null || ob.CompareTo(great) > 0)
					great = ob;
			}

			return great;
		}

		public static TObject Least(TObject[] args) {
			TObject least = null;
			foreach (TObject ob in args) {
				if (ob.IsNull)
					return ob;

				if (least == null || ob.CompareTo(least) < 0)
					least = ob;
			}
			return least;
		}

		public static TObject Cast(TObject ob, TObject typeString) {
			var encodedType = typeString.Object.ToString();
			var castToType = TType.DecodeString(encodedType);

			// If types are the same then no cast is necessary and we return this
			// object.
			if (ob.TType.SqlType == castToType.SqlType)
				return ob;

			// Otherwise cast the object and return the new typed object.
			Object castedOb = TType.CastObjectToTType(ob.Object, castToType);
			return new TObject(castToType, castedOb);
		}

		public static TObject ToNumber(TObject ob) {
			return Cast(ob, TObject.CreateString(PrimitiveTypes.Numeric.ToString()));
		}

		public static TObject Version() {
			Version version = ProductInfo.Current.Version;
			return TObject.CreateString(version.ToString(2));
		}

		public static TObject Exists(TObject ob, IQueryContext context) {
			if (ob.IsNull)
				return TObject.CreateBoolean(false);

			if (!(ob.TType is TQueryPlanType))
				throw new InvalidOperationException("The EXISTS function must have a query argument.");

			var plan = ob.Object as IQueryPlanNode;
			if (plan == null)
				throw new InvalidOperationException();

			Table table = plan.Evaluate(context);
			return TObject.CreateBoolean(table.RowCount > 0);
		}

		public static TObject IsUnique(TObject ob, IQueryContext context) {
			if (ob.IsNull)
				return TObject.CreateBoolean(false);

			if (!(ob.TType is TQueryPlanType))
				throw new InvalidOperationException("The UNIQUE function must have a query argument.");

			var plan = ob.Object as IQueryPlanNode;
			if (plan == null)
				throw new InvalidOperationException();

			var table = plan.Evaluate(context);
			return TObject.CreateBoolean(table.RowCount == 1);
		}

		public static TObject ViewData(TObject commandObj, TObject data) {
			// Get the parameters.  The first is a string describing the operation.
			// The second is the binary data to process and output the information
			// for.
			String commandStr = commandObj.Object.ToString();
			var blob = (ByteLongObject) data.Object;

			if (String.Compare(commandStr, "referenced tables", StringComparison.OrdinalIgnoreCase) == 0) {
				View view = View.DeserializeFromBlob(blob);
				IQueryPlanNode node = view.QueryPlanNode;
				IList<TableName> touchedTables = node.DiscoverTableNames(new List<TableName>());
				var buf = new StringBuilder();
				int sz = touchedTables.Count;
				for (int i = 0; i < sz; ++i) {
					buf.Append(touchedTables[i]);

					if (i < sz - 1)
						buf.Append(", ");
				}
				return TObject.CreateString(buf.ToString());
			}
			if (String.Compare(commandStr, "plan dump", StringComparison.OrdinalIgnoreCase) == 0) {
				View view = View.DeserializeFromBlob(blob);
				IQueryPlanNode node = view.QueryPlanNode;
				var buf = new StringBuilder();
				node.DebugString(0, buf);
				return TObject.CreateString(buf.ToString());
			}
			if (String.Compare(commandStr, "query string", StringComparison.OrdinalIgnoreCase) == 0) {
				SqlQuery query = SqlQuery.DeserializeFromBlob(blob);
				return TObject.CreateString(query.ToString());
			}

			return TObject.Null;
		}

		#region Crc32Hash

		private class Crc32Hash : HashAlgorithm {
			public const uint DefaultSeed = 0xffffffff;

			private static readonly uint[] CrcTable = new uint[] {
				0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA, 0x076DC419,
				0x706AF48F, 0xE963A535, 0x9E6495A3, 0x0EDB8832, 0x79DCB8A4,
				0xE0D5E91E, 0x97D2D988, 0x09B64C2B, 0x7EB17CBD, 0xE7B82D07,
				0x90BF1D91, 0x1DB71064, 0x6AB020F2, 0xF3B97148, 0x84BE41DE,
				0x1ADAD47D, 0x6DDDE4EB, 0xF4D4B551, 0x83D385C7, 0x136C9856,
				0x646BA8C0, 0xFD62F97A, 0x8A65C9EC, 0x14015C4F, 0x63066CD9,
				0xFA0F3D63, 0x8D080DF5, 0x3B6E20C8, 0x4C69105E, 0xD56041E4,
				0xA2677172, 0x3C03E4D1, 0x4B04D447, 0xD20D85FD, 0xA50AB56B,
				0x35B5A8FA, 0x42B2986C, 0xDBBBC9D6, 0xACBCF940, 0x32D86CE3,
				0x45DF5C75, 0xDCD60DCF, 0xABD13D59, 0x26D930AC, 0x51DE003A,
				0xC8D75180, 0xBFD06116, 0x21B4F4B5, 0x56B3C423, 0xCFBA9599,
				0xB8BDA50F, 0x2802B89E, 0x5F058808, 0xC60CD9B2, 0xB10BE924,
				0x2F6F7C87, 0x58684C11, 0xC1611DAB, 0xB6662D3D, 0x76DC4190,
				0x01DB7106, 0x98D220BC, 0xEFD5102A, 0x71B18589, 0x06B6B51F,
				0x9FBFE4A5, 0xE8B8D433, 0x7807C9A2, 0x0F00F934, 0x9609A88E,
				0xE10E9818, 0x7F6A0DBB, 0x086D3D2D, 0x91646C97, 0xE6635C01,
				0x6B6B51F4, 0x1C6C6162, 0x856530D8, 0xF262004E, 0x6C0695ED,
				0x1B01A57B, 0x8208F4C1, 0xF50FC457, 0x65B0D9C6, 0x12B7E950,
				0x8BBEB8EA, 0xFCB9887C, 0x62DD1DDF, 0x15DA2D49, 0x8CD37CF3,
				0xFBD44C65, 0x4DB26158, 0x3AB551CE, 0xA3BC0074, 0xD4BB30E2,
				0x4ADFA541, 0x3DD895D7, 0xA4D1C46D, 0xD3D6F4FB, 0x4369E96A,
				0x346ED9FC, 0xAD678846, 0xDA60B8D0, 0x44042D73, 0x33031DE5,
				0xAA0A4C5F, 0xDD0D7CC9, 0x5005713C, 0x270241AA, 0xBE0B1010,
				0xC90C2086, 0x5768B525, 0x206F85B3, 0xB966D409, 0xCE61E49F,
				0x5EDEF90E, 0x29D9C998, 0xB0D09822, 0xC7D7A8B4, 0x59B33D17,
				0x2EB40D81, 0xB7BD5C3B, 0xC0BA6CAD, 0xEDB88320, 0x9ABFB3B6,
				0x03B6E20C, 0x74B1D29A, 0xEAD54739, 0x9DD277AF, 0x04DB2615,
				0x73DC1683, 0xE3630B12, 0x94643B84, 0x0D6D6A3E, 0x7A6A5AA8,
				0xE40ECF0B, 0x9309FF9D, 0x0A00AE27, 0x7D079EB1, 0xF00F9344,
				0x8708A3D2, 0x1E01F268, 0x6906C2FE, 0xF762575D, 0x806567CB,
				0x196C3671, 0x6E6B06E7, 0xFED41B76, 0x89D32BE0, 0x10DA7A5A,
				0x67DD4ACC, 0xF9B9DF6F, 0x8EBEEFF9, 0x17B7BE43, 0x60B08ED5,
				0xD6D6A3E8, 0xA1D1937E, 0x38D8C2C4, 0x4FDFF252, 0xD1BB67F1,
				0xA6BC5767, 0x3FB506DD, 0x48B2364B, 0xD80D2BDA, 0xAF0A1B4C,
				0x36034AF6, 0x41047A60, 0xDF60EFC3, 0xA867DF55, 0x316E8EEF,
				0x4669BE79, 0xCB61B38C, 0xBC66831A, 0x256FD2A0, 0x5268E236,
				0xCC0C7795, 0xBB0B4703, 0x220216B9, 0x5505262F, 0xC5BA3BBE,
				0xB2BD0B28, 0x2BB45A92, 0x5CB36A04, 0xC2D7FFA7, 0xB5D0CF31,
				0x2CD99E8B, 0x5BDEAE1D, 0x9B64C2B0, 0xEC63F226, 0x756AA39C,
				0x026D930A, 0x9C0906A9, 0xEB0E363F, 0x72076785, 0x05005713,
				0x95BF4A82, 0xE2B87A14, 0x7BB12BAE, 0x0CB61B38, 0x92D28E9B,
				0xE5D5BE0D, 0x7CDCEFB7, 0x0BDBDF21, 0x86D3D2D4, 0xF1D4E242,
				0x68DDB3F8, 0x1FDA836E, 0x81BE16CD, 0xF6B9265B, 0x6FB077E1,
				0x18B74777, 0x88085AE6, 0xFF0F6A70, 0x66063BCA, 0x11010B5C,
				0x8F659EFF, 0xF862AE69, 0x616BFFD3, 0x166CCF45, 0xA00AE278,
				0xD70DD2EE, 0x4E048354, 0x3903B3C2, 0xA7672661, 0xD06016F7,
				0x4969474D, 0x3E6E77DB, 0xAED16A4A, 0xD9D65ADC, 0x40DF0B66,
				0x37D83BF0, 0xA9BCAE53, 0xDEBB9EC5, 0x47B2CF7F, 0x30B5FFE9,
				0xBDBDF21C, 0xCABAC28A, 0x53B39330, 0x24B4A3A6, 0xBAD03605,
				0xCDD70693, 0x54DE5729, 0x23D967BF, 0xB3667A2E, 0xC4614AB8,
				0x5D681B02, 0x2A6F2B94, 0xB40BBE37, 0xC30C8EA1, 0x5A05DF1B,
				0x2D02EF8D
			};

			private uint crcValue = 0;

			public override void Initialize() {
				crcValue = 0;
			}

			protected override void HashCore(byte[] buffer, int start, int length) {
				crcValue ^= DefaultSeed;

				unchecked {
					while (--length >= 0) {
						crcValue = CrcTable[(crcValue ^ buffer[start++]) & 0xFF] ^ (crcValue >> 8);
					}
				}

				crcValue ^= DefaultSeed;
			}

			protected override byte[] HashFinal() {
				this.HashValue = new byte[] {
					(byte) ((crcValue >> 24) & 0xff),
					(byte) ((crcValue >> 16) & 0xff),
					(byte) ((crcValue >> 8) & 0xff),
					(byte) (crcValue & 0xff)
				};
				return this.HashValue;
			}

			public uint CrcValue {
				get { return (uint) ((HashValue[0] << 24) | (HashValue[1] << 16) | (HashValue[2] << 8) | HashValue[3]); }
			}

			public override int HashSize {
				get { return 32; }
			}
		}

		#endregion

		#region Adler32Hash

		private class Adler32Hash : HashAlgorithm {
			public Adler32Hash() {
				Initialize();
			}

			private ushort sum_1;
			private ushort sum_2;

			#region Overrides of HashAlgorithm

			public override int HashSize {
				get { return 32; }
			}

			public override void Initialize() {
				sum_1 = 1;
				sum_2 = 0;
			}

			protected override void HashCore(byte[] array, int ibStart, int cbSize) {
				// process each byte in the array
				for (int i = ibStart; i < cbSize; i++) {
					sum_1 = (ushort) ((sum_1 + array[i])%65521);
					sum_2 = (ushort) ((sum_1 + sum_2)%65521);
				}
			}

			protected override byte[] HashFinal() {
				// concat the two 16 bit values to form
				// one 32-bit value
				uint value = (uint) ((sum_2 << 16) | sum_1);
				// use the bitconverter class to render the
				// 32-bit integer into an array of bytes
				return BitConverter.GetBytes(value);
			}

			#endregion
		}

		#endregion
	}
}