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
using System.Collections.Generic;
using System.Globalization;
using System.Text;

using Deveel.Data.DbSystem;
using Deveel.Data.Query;
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

		private static ExecuteResult CurrentUser(ExecuteContext context) {
			return context.FunctionResult(TObject.CreateString(context.QueryContext.UserName));
		}

		private static TObject Evaluate(this Expression exp, ExecuteContext context) {
			return exp.Evaluate(context.GroupResolver, context.VariableResolver, context.QueryContext);
		}

		private static TType ReturnTType(this Expression exp, ExecuteContext context) {
			return exp.ReturnTType(context.VariableResolver, context.QueryContext);
		}

		#region Iif

		private static ExecuteResult Iif(ExecuteContext context) {
			TObject result = TObject.Null;

			var condition = Evaluate(context.Arguments[0], context);
			if (condition.TType is TBooleanType) {
				// Does the result equal true?
				if (condition.CompareTo(TObject.BooleanTrue) == 0) {
					// Resolved to true so evaluate the first argument
					result = Evaluate(context.Arguments[1], context);
				} else {
					// Otherwise result must evaluate to NULL or false, so evaluate
					// the second parameter
					result = Evaluate(context.Arguments[2], context);
				}
			}
			// Result was not a bool so return null
			return context.FunctionResult(result);
		}

		private static TType IifReturnType(ExecuteContext context) {
			// It's impossible to know the return type of this function until runtime
			// because either comparator could be returned.  We could assume that
			// both branch expressions result in the same type of object but this
			// currently is not enforced.

			// Returns type of first argument
			TType t1 = context.Arguments[1].ReturnTType(context);
			// This is a hack for null values.  If the first parameter is null
			// then return the type of the second parameter which hopefully isn't
			// also null.
			if (t1 is TNullType) {
				return context.Arguments[2].ReturnTType(context);
			}
			return t1;
		}

		#endregion

		#region ForeignRuleConvert

		private static TObject FRuleConvert(TObject[] arg) {
			return FRuleConvert(arg[0]);
		}

		public static TObject FRuleConvert(TObject obj) {
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
				if (code == (int)ConstraintAction.Cascade) {
					v = "CASCADE";
				} else if (code == (int)ConstraintAction.NoAction) {
					v = "NO ACTION";
				} else if (code == (int)ConstraintAction.SetDefault) {
					v = "SET DEFAULT";
				} else if (code == (int)ConstraintAction.SetNull) {
					v = "SET NULL";
				} else {
					throw new ApplicationException("Unrecognised foreign key rule: " + code);
				}

				return TObject.CreateString(v);
			}

			throw new ApplicationException("Unsupported type in function argument");
		}

		#endregion

		#region UniqueKey

		private static ExecuteResult UniqueKey(ExecuteContext context) {
			return context.FunctionResult(UniqueKey(context.QueryContext, context.EvaluatedArguments[0]));
		}

		public static TObject UniqueKey(IQueryContext context, TObject tableName) {
			var str = tableName.Object.ToString();
			long v = context.NextSequenceValue(str);
			return TObject.CreateInt8(v);
		}

		#endregion

		#region Aggregate Functions

		public static TObject Or(TObject ob1, TObject ob2) {
			return ob1 != null ? (ob2.IsNull ? ob1 : (!ob1.IsNull ? ob1.Or(ob2) : ob2)) : ob2;
		}

		private static ExecuteResult Count(ExecuteContext context) {
			if (context.GroupResolver == null)
				throw new Exception("'count' can only be used as an aggregate function.");

			int size = context.GroupResolver.Count;
			TObject result;
			// if, count(*)
			if (size == 0 || context.Invoke.IsGlobArguments) {
				result = TObject.CreateInt4(size);
			} else {
				// Otherwise we need to count the number of non-null entries in the
				// columns list(s).

				int totalCount = size;

				Expression exp = context.Arguments[0];
				for (int i = 0; i < size; ++i) {
					TObject val = exp.Evaluate(null, context.GroupResolver.GetVariableResolver(i), context.QueryContext);
					if (val.IsNull) {
						--totalCount;
					}
				}

				result = TObject.CreateInt4(totalCount);
			}

			return context.FunctionResult(result);
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

		private static ExecuteResult DistinctCount(ExecuteContext context) {
			// There's some issues with implementing this function.
			// For this function to be efficient, we need to have access to the
			// underlying Table object(s) so we can use table indexing to sort the
			// columns.  Otherwise, we will need to keep in memory the group
			// contents so it can be sorted.  Or alternatively (and probably worst
			// of all) don't store in memory, but use an expensive iterative search
			// for non-distinct rows.
			//
			// An iterative search will be terrible for large groups with mostly
			// distinct rows.  But would be okay for large groups with few distinct
			// rows.

			if (context.GroupResolver == null)
				throw new Exception("'count' can only be used as an aggregate function.");

			int rows = context.GroupResolver.Count;
			if (rows <= 1) {
				// If count of entries in group is 0 or 1
				return context.FunctionResult(TObject.CreateInt4(rows));
			}

			// Make an array of all cells in the group that we are finding which
			// are distinct.
			int cols = context.ArgumentCount;
			TObject[] groupRow = new TObject[rows * cols];
			int n = 0;
			for (int i = 0; i < rows; ++i) {
				IVariableResolver vr = context.GroupResolver.GetVariableResolver(i);
				for (int p = 0; p < cols; ++p) {
					Expression exp = context.Arguments[p];
					groupRow[n + p] = exp.Evaluate(null, vr, context.QueryContext);
				}
				n += cols;
			}

			// A comparator that sorts this set,
			IComparer c = new DistinctComparer(cols, groupRow);

			// The list of indexes,
			object[] list = new object[rows];
			for (int i = 0; i < rows; ++i) {
				list[i] = i;
			}

			// Sort the list,
			Array.Sort(list, c);

			// The count of distinct elements, (there will always be at least 1)
			int distinctCount = 1;
			for (int i = 1; i < rows; ++i) {
				int v = c.Compare(list[i], list[i - 1]);
				// If v == 0 then entry is not distinct with the previous element in
				// the sorted list therefore the distinct counter is not incremented.
				if (v > 0) {
					// If current entry is greater than previous then we've found a
					// distinct entry.
					++distinctCount;
				} else if (v < 0) {
					// The current element should never be less if list is sorted in
					// ascending order.
					throw new ApplicationException("Assertion failed - the distinct list does not " +
												   "appear to be sorted.");
				}
			}

			// If the first entry in the list is NULL then subtract 1 from the
			// distinct count because we shouldn't be counting NULL entries.
			if (list.Length > 0) {
				int firstEntry = (int)list[0];
				// Assume first is null
				bool firstIsNull = true;
				for (int m = 0; m < cols && firstIsNull == true; ++m) {
					TObject val = groupRow[(firstEntry * cols) + m];
					if (!val.IsNull) {
						// First isn't null
						firstIsNull = false;
					}
				}
				// Is first NULL?
				if (firstIsNull) {
					// decrease distinct count so we don't count the null entry.
					distinctCount = distinctCount - 1;
				}
			}

			return context.FunctionResult(TObject.CreateInt4(distinctCount));
		}

		private class DistinctComparer : IComparer {
				private readonly int cols;
				private readonly TObject[] groupRow;

				public DistinctComparer(int cols, TObject[] groupRow) {
					this.cols = cols;
					this.groupRow = groupRow;
				}

				public int Compare(Object ob1, Object ob2) {
					int r1 = (int)ob1;
					int r2 = (int)ob2;

					// Compare row r1 with r2
					int index1 = r1 * cols;
					int index2 = r2 * cols;
					for (int n = 0; n < cols; ++n) {
						int v = groupRow[index1 + n].CompareTo(groupRow[index2 + n]);
						if (v != 0) {
							return v;
						}
					}

					// If we got here then rows must be equal.
					return 0;
				} 
		}

		#endregion

		#region Arithmetic Functions

		public static TObject Sqrt(TObject ob) {
			if (ob.IsNull)
				return ob;

			return TObject.CreateBigNumber(ob.ToBigNumber().Sqrt());
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

		#endregion

		#region Date/Time Functions

		private static int ExtractField(string field, TObject obj) {
			DateTime dateTime = DateTime.MinValue;
			Interval timeSpan = Interval.Zero;
			bool fromTs = false;

			if (obj.TType is TDateType) {
				dateTime = obj.ToDateTime();
			} else if (obj.TType is TIntervalType) {
				timeSpan = obj.ToInterval();
				fromTs = true;
			} else {
				obj = obj.CastTo(PrimitiveTypes.Date);
				dateTime = obj.ToDateTime();
			}

			int value;

			if (fromTs) {
				switch (field) {
					case "year": value = timeSpan.Days; break;
					case "month": value = timeSpan.Months; break;
					case "day": value = timeSpan.Days; break;
					case "hour": value = timeSpan.Hours; break;
					case "minute": value = timeSpan.Minutes; break;
					case "second": value = timeSpan.Seconds; break;
					default: throw new InvalidOperationException("Field " + field + " not supported in an INTERVAL type.");
				}
			} else {
				switch (field) {
					case "year": value = dateTime.Year; break;
					case "month": value = dateTime.Month; break;
					case "day": value = dateTime.Day; break;
					case "hour": value = dateTime.Hour; break;
					case "minute": value = dateTime.Minute; break;
					case "second": value = dateTime.Second; break;
					default: throw new InvalidOperationException("Field " + field + " not supported in a TIME type.");
				}
			}

			return value;
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

		public static TObject AddMonths(TObject ob, TObject n) {
			if (ob.IsNull || !(ob.TType is TDateType))
				return ob;

			if (n.IsNull)
				return ob;

			DateTime date = ob.ToDateTime();
			int value = n.ToBigNumber().ToInt32();

			return TObject.CreateDateTime(date.AddMonths(value));
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

		private static DayOfWeek GetDayOfWeek(TObject ob) {
			if (ob.TType is TNumericType)
				return (DayOfWeek)ob.ToBigNumber().ToInt32();
			return (DayOfWeek)Enum.Parse(typeof(DayOfWeek), ob.ToStringValue(), true);
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

			int c = (int)current;
			int d = (int)desired;
			int n = (7 - c + d);

			return (n > 7) ? n % 7 : n;
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

		private static TType ConcatReturnTType(ExecuteContext context) {
			// Determine the locale of the first string parameter.
			CultureInfo locale = null;
			CollationStrength strength = 0;
			CollationDecomposition decomposition = 0;
			for (int i = 0; i < context.ArgumentCount && locale == null; ++i) {
				TType type = context.Arguments[i].ReturnTType(context);
				if (type is TStringType) {
					TStringType strType = (TStringType)type;
					locale = strType.Locale;
					strength = strType.Strength;
					decomposition = strType.Decomposition;
				}
			}

			return locale != null ? new TStringType(SqlType.VarChar, -1, locale, strength, decomposition)
				: PrimitiveTypes.VarString;
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
			int strLength = str.Length;

			return Substring(ob, startOffset, TObject.CreateInt4(strLength));
		}

		public static TObject Substring(TObject ob, TObject startOffset, TObject endOffset) {
			if (ob.IsNull)
				return ob;

			string str = ob.Object.ToString();
			int strLength = str.Length;
			int start = startOffset.ToBigNumber().ToInt32();
			int end = endOffset.ToBigNumber().ToInt32();

			// Make sure this call is safe for all lengths of string.
			if (start < 1)
				start = 1;

			if (start > strLength)
				return TObject.CreateString("");

			if (end + start > strLength)
				end = (strLength - start);

			if (end < 1)
				return TObject.CreateString("");

			return TObject.CreateString(str.Substring(start - 1, (end - start) - 1));
		}

		public static TObject InString(TObject ob, TObject search) {
			return InString(ob, search, TObject.CreateInt4(1));
		}

		public static TObject InString(TObject ob, TObject search, TObject startOffset) {
			if (ob.IsNull)
				return ob;

			string str = ob.Object.ToString();
			int strLength = str.Length;

			return InString(ob, search, startOffset, TObject.CreateInt4(strLength));
		}

		public static TObject InString(TObject ob, TObject search, TObject startOffset, TObject endOffset) {
			return TObject.Null;
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

			var s = (IStringAccessor)ob.Object;
			if (s == null)
				return TObject.Null;

			return TObject.CreateInt4(s.Length);
		}

		public static TObject Soundex(TObject ob) {
			if (ob.IsNull)
				return ob;

			return TObject.CreateString(Text.Soundex.UsEnglish.Compute(ob.ToStringValue()));
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

			var locale = ((TStringType)ob.TType).Locale;
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

		public static TObject Coalesce(TObject[] args) {
			int count = args.Length;
			for (int i = 0; i < count - 1; ++i) {
				TObject res  = args[i];
				if (!res.IsNull) {
					return res;
				}
			}

			return args[count - 1];
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

		public static TObject ViewData(TObject commandObj, TObject data) {
			// Get the parameters.  The first is a string describing the operation.
			// The second is the binary data to process and output the information
			// for.
			String commandStr = commandObj.Object.ToString();
			var blob = (ByteLongObject)data.Object;

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

		#region SystemFunctionsFactory

		class SystemFunctionsFactory : FunctionFactory {
			protected override void OnInit() {
				// Aggregate Functions
				Build("aggor", DynamicUnbounded(0), BuildAggOr, FunctionType.Aggregate);
				Build("avg", DynamicUnbounded(0), BuildAvg, FunctionType.Aggregate);
				Build("count", Dynamic(0), BuildCount, FunctionType.Aggregate);
				Build("max", DynamicUnbounded(0), BuildMax, FunctionType.Aggregate);
				Build("min", DynamicUnbounded(0), BuildMin, FunctionType.Aggregate);
				Build("sum", DynamicUnbounded(0), BuildSum, FunctionType.Aggregate);
				Build("distinct_count", DynamicUnbounded(0), BuildDistCount, FunctionType.Aggregate);

				// Security Functions
				Build("user", builder => builder.ReturnsType(PrimitiveTypes.VarString).OnExecute(CurrentUser));

				// Sequence Functions
				Build("uniquekey", Parameter(0, PrimitiveTypes.VarString), builder => builder.OnExecute(UniqueKey));

				// Misc Functions
				Build("iif", new[]{ Parameter(0, PrimitiveTypes.Boolean), Dynamic(1), Dynamic(2)}, BuildIif);
				Build("sql_cast", new[]{Parameter(0, Function.DynamicType), Parameter(1, PrimitiveTypes.VarString)}, BuildSqlCast);
				Build("version", BuildVersion);
				Build("sql_exists", Parameter(0, PrimitiveTypes.QueryPlan), BuildExists);

				Build("coalesce", DynamicUnbounded(0), BuildCoalesce);
				Build("greatest", DynamicUnbounded(0), BuildGreatest);
				Build("least", DynamicUnbounded(0), BuildLeast);

				// Internal
				Build("i_frule_convert", Dynamic(0), BuildForeignRuleConvert);
				Build("i_view_data",
					new[] {
						Parameter(0, PrimitiveTypes.VarString),
						Parameter(1, PrimitiveTypes.BinaryType)
					}, BuildViewData);

				// Aithmetic Functions

				// String Functions
				Build("concat", Unbounded(0, PrimitiveTypes.VarString), BuildConcat);
				Build("replace",
					new[] {
						Parameter(0, PrimitiveTypes.VarString), 
						Parameter(1, PrimitiveTypes.VarString), 
						Parameter(2, PrimitiveTypes.VarString)
					},
					BuildReplace);

				Build("substring", 
					new[] {
						Parameter(0, PrimitiveTypes.VarString), 
						Parameter(1, PrimitiveTypes.Numeric)
					}, BuildSubstring1);
				Build("substring",
					new[] {
						Parameter(0, PrimitiveTypes.VarString), 
						Parameter(1, PrimitiveTypes.Numeric), 
						Parameter(2, PrimitiveTypes.Numeric)
					},
					BuildSubstring2);
				Build("instr",
					new [] {
						Parameter(0, PrimitiveTypes.VarString),
						Parameter(1, PrimitiveTypes.VarString)
					}, BuildInString1);
				Build("instr",
					new[] {
						Parameter(0, PrimitiveTypes.VarString),
						Parameter(1, PrimitiveTypes.VarString),
						Parameter(2, PrimitiveTypes.Numeric)
					}, BuildInString2);
				Build("instr",
					new[] {
						Parameter(0, PrimitiveTypes.VarString),
						Parameter(1, PrimitiveTypes.VarString),
						Parameter(2, PrimitiveTypes.Numeric),
						Parameter(3, PrimitiveTypes.Numeric)
					}, BuildInString3);

				Build("lpad",
					new[] {
						Parameter(0, PrimitiveTypes.VarString),
						Parameter(1, PrimitiveTypes.Numeric)
					}, BuildLPad1);
				Build("lpad",
					new[] {
						Parameter(0, PrimitiveTypes.VarString),
						Parameter(1, PrimitiveTypes.Numeric),
						Parameter(2, PrimitiveTypes.VarString)
					}, BuildLPad2);

				Build("rpad",
					new[] {
						Parameter(0, PrimitiveTypes.VarString),
						Parameter(1, PrimitiveTypes.Numeric)
					}, BuildRPad1);
				Build("rpad",
					new[] {
						Parameter(0, PrimitiveTypes.VarString),
						Parameter(1, PrimitiveTypes.Numeric),
						Parameter(2, PrimitiveTypes.VarString)
					}, BuildRPad2);
				Build("char_length", Parameter(0, PrimitiveTypes.VarString), BuildCharLength);
				Build("soundex", Parameter(0, PrimitiveTypes.VarString), BuildSoundex);
				Build("upper", Parameter(0, PrimitiveTypes.VarString), BuildUpper);
				Build("lower", Parameter(0, PrimitiveTypes.VarString), BuildLower);
				Build("sql_trim",
					new[] {
						Parameter(0, PrimitiveTypes.VarString),
						Parameter(1, PrimitiveTypes.VarString),
						Parameter(2, PrimitiveTypes.VarString)
					}, BuildTrim);
				Build("ltrim",
					new[] {
						Parameter(0, PrimitiveTypes.VarString),
						Parameter(1, PrimitiveTypes.VarString)
					}, BuildLTrim1);
				Build("ltrim",
					new[] {
						Parameter(0, PrimitiveTypes.VarString),
					}, BuildLTrim2);
				Build("rtrim",
					new[] {
						Parameter(0, PrimitiveTypes.VarString),
						Parameter(1, PrimitiveTypes.VarString)
					}, BuildRTrim1);
				Build("rtrim", Parameter(0, PrimitiveTypes.VarString), BuildRTrim2);

				// Arithmetic Functions
				Build("sqrt", Parameter(0, PrimitiveTypes.Numeric), BuildSqrt);
				Build("log", Parameter(0, PrimitiveTypes.Numeric), BuildLog1);
				Build("log", new []{Parameter(0, PrimitiveTypes.Numeric), Parameter(1, PrimitiveTypes.Numeric)}, BuildLog2);
				Build("log10", Parameter(0, PrimitiveTypes.Numeric), BuildLog10);
				Build("tan", Parameter(0, PrimitiveTypes.Numeric), BuildTan);
				Build("tanh", Parameter(0, PrimitiveTypes.Numeric), BuildTanH);
				Build("round", Parameter(0, PrimitiveTypes.Numeric), BuildRound1);
				Build("round", new[]{Parameter(0, PrimitiveTypes.Numeric), Parameter(1, PrimitiveTypes.VarString)}, BuildRound2);
				Build("pow", Parameter(0, PrimitiveTypes.Numeric), BuildPow);

				// Date/Time Functions
				Build("dateob", new[]{Parameter(0, PrimitiveTypes.VarString)}, BuildDate);
				Build("dateob", BuildDate2);
				Build("timeob", Parameter(0, PrimitiveTypes.VarString), BuildTime);
				Build("timeob", BuildTime2);
				Build("timestampob", Parameter(0, PrimitiveTypes.VarString), BuildTimestamp);
				Build("timestampob", BuildTimestamp2);
				Build("dbtimezone", BuildDbTimeZone);
			}

			#region Function Builders

			private static IFunction BuildIif(RoutineInfo info) {
				return FunctionBuilder.New(info.Name)
					.WithParameters(info.Parameters)
					.OnReturnType(IifReturnType)
					.OnExecute(Iif);
			}

			private static IFunction BuildForeignRuleConvert(RoutineInfo info) {
				return FunctionBuilder.New(info.Name)
					.WithParameters(info.Parameters)
					.OnReturnType(FRuleConvertReturnType)
					.OnExecute(FRuleConvert);
			}

			private static void BuildAggOr(FunctionBuilder builder) {
				// Assuming bitmap numbers, this will find the result of or'ing all the
				// values in the aggregate set.
				builder.OnAggregate((group, resolver, ob1, ob2) => Or(ob1, ob2));
			}

			private static void BuildAvg(FunctionBuilder builder) {
				builder
					.OnAggregate((resolver, variableResolver, ob1, ob2) => ob1 != null ? (ob2.IsNull ? ob1 : (!ob1.IsNull ? ob1.Add(ob2) : ob2)) : ob2)
					.OnAfterAggregate((group, resolver, result) => result.IsNull ? result : result.Divide(TObject.CreateInt4(@group.Count)));
			}

			private static void BuildCount(FunctionBuilder builder) {
				builder.OnExecute(Count);
			}

			private static void BuildMax(FunctionBuilder builder) {
				builder.OnAggregate((group, resolver, ob1, ob2) => Max(ob1, ob2))
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildMin(FunctionBuilder builder) {
				builder.OnAggregate((group, resolver, ob1, ob2) => Min(ob1, ob2))
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildSum(FunctionBuilder builder) {
				builder.OnAggregate((group, resolver, ob1, ob2) => Sum(ob1, ob2));
			}

			private static void BuildDistCount(FunctionBuilder builder) {
				builder.OnExecute(DistinctCount);
			}

			private static void BuildConcat(FunctionBuilder builder) {
				builder.OnExecute(Concat)
					.OnReturnType(ConcatReturnTType);
			}

			private static void BuildReplace(FunctionBuilder builder) {
				builder.OnExecute(context => context.FunctionResult(Replace(context.EvaluatedArguments[0], context.EvaluatedArguments[1], context.EvaluatedArguments[2])))
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildSubstring1(FunctionBuilder builder) {
				builder.OnExecute(args => Substring(args[0], args[1]))
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildSubstring2(FunctionBuilder builder) {
				builder.OnExecute(args => Substring(args[0], args[1], args[2]))
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildInString1(FunctionBuilder builder) {
				builder.OnExecute(args => InString(args[0], args[1]))
					.ReturnsType(PrimitiveTypes.Numeric);
			}

			private static void BuildInString2(FunctionBuilder builder) {
				builder.OnExecute(args => InString(args[0], args[1], args[2]))
					.ReturnsType(PrimitiveTypes.Numeric);
			}

			private static void BuildInString3(FunctionBuilder builder) {
				builder.OnExecute(args => InString(args[0], args[1], args[2], args[3]))
					.ReturnsType(PrimitiveTypes.Numeric);
			}

			private static void BuildLPad1(FunctionBuilder builder) {
				builder.OnExecute(args => LPad(args[0], args[1]))
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildLPad2(FunctionBuilder builder) {
				builder.OnExecute(args => LPad(args[0], args[1], args[2]))
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildRPad1(FunctionBuilder builder) {
				builder.OnExecute(args => RPad(args[0], args[1]))
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildRPad2(FunctionBuilder builder) {
				builder.OnExecute(args => RPad(args[0], args[1], args[2]))
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildCharLength(FunctionBuilder builder) {
				builder.OnExecute(args => CharLength(args[0]))
					.ReturnsType(PrimitiveTypes.Numeric);
			}

			private static void BuildSoundex(FunctionBuilder builder) {
				builder.OnExecute(args => Soundex(args[0]))
					.ReturnsType(PrimitiveTypes.VarString);
			}

			private static void BuildLower(FunctionBuilder builder) {
				builder.OnExecute(args => Lower(args[0]))
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildUpper(FunctionBuilder builder) {
				builder.OnExecute(args => Upper(args[0]))
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildTrim(FunctionBuilder builder) {
				builder.OnExecute(args => Trim(args[2], args[0], args[1]))
					.OnReturnType(context => context.Arguments[2].ReturnTType(context));
			}

			private static void BuildLTrim1(FunctionBuilder builder) {
				builder.OnExecute(args => LTrim(args[0], args[1]))
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildLTrim2(FunctionBuilder builder) {
				builder.OnExecute(args => LTrim(args[0]))
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildRTrim1(FunctionBuilder builder) {
				builder.OnExecute(args => RTrim(args[0], args[1]))
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildRTrim2(FunctionBuilder builder) {
				builder.OnExecute(args => RTrim(args[0]))
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildSqrt(FunctionBuilder builder) {
				builder.OnExecute(args => Sqrt(args[0]))
					.ReturnsType(PrimitiveTypes.Numeric);
			}

			private static void BuildTan(FunctionBuilder builder) {
				builder.OnExecute(args => Tan(args[0]))
					.ReturnsType(PrimitiveTypes.Numeric);
			}

			private static void BuildTanH(FunctionBuilder builder) {
				builder.OnExecute(args => TanH(args[0]))
					.ReturnsType(PrimitiveTypes.Numeric);
			}

			private static void BuildPow(FunctionBuilder builder) {
				builder.OnExecute(args => Pow(args[0], args[1]))
					.ReturnsType(PrimitiveTypes.Numeric);
			}

			private static void BuildRound1(FunctionBuilder builder) {
				builder.OnExecute(args => Round(args[0], args[1]))
					.ReturnsType(PrimitiveTypes.Numeric);
			}

			private static void BuildRound2(FunctionBuilder builder) {
				builder.OnExecute(args => Round(args[0]))
					.ReturnsType(PrimitiveTypes.Numeric);
			}

			private static void BuildLog1(FunctionBuilder builder) {
				builder.OnExecute(args => Log(args[0], args[1]))
					.ReturnsType(PrimitiveTypes.Numeric);
			}

			private static void BuildLog2(FunctionBuilder builder) {
				builder.OnExecute(args => Log(args[0]))
					.ReturnsType(PrimitiveTypes.Numeric);
			}

			private static void BuildLog10(FunctionBuilder builder) {
				builder.OnExecute(args => Log10(args[0]))
					.ReturnsType(PrimitiveTypes.Numeric);
			}

			private static void BuildDate(FunctionBuilder builder) {
				builder.OnExecute(args => ToDate(args[0]))
					.ReturnsType(new TDateType(SqlType.Date));
			}

			private static void BuildDate2(FunctionBuilder builder) {
				builder.OnExecute(args => CurrentDate())
					.ReturnsType(TType.GetDateType(SqlType.Date));
			}

			private static void BuildTimestamp(FunctionBuilder builder) {
				builder.OnExecute(args => ToTimeStamp(args[0]))
					.ReturnsType(new TDateType(SqlType.TimeStamp));
			}

			private static void BuildTimestamp2(FunctionBuilder build) {
				build.OnExecute(args => CurrentTimestamp())
					.ReturnsType(TType.GetDateType(SqlType.TimeStamp));
			}

			private static void BuildTime(FunctionBuilder builder) {
				builder.OnExecute(args => ToTime(args[0]))
					.ReturnsType(new TDateType(SqlType.Time));
			}

			private static void BuildTime2(FunctionBuilder build) {
				build.OnExecute(args => CurrentTime())
					.ReturnsType(TType.GetDateType(SqlType.Time));
			}

			private static void BuildAddMonths(FunctionBuilder build) {
				build.OnExecute(args => AddMonths(args[0], args[1]))
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildNextDay(FunctionBuilder builder) {
				builder.OnExecute(args => NextDay(args[0], args[1]))
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildDbTimeZone(FunctionBuilder builder) {
				builder.OnExecute(args => DbTimeZone())
					.ReturnsType(PrimitiveTypes.VarString);
			}

			private static void BuildSqlCast(FunctionBuilder builder) {
				builder.OnExecute(args => Cast(args[0], args[1]))
					.OnReturnType(CastReturnType);
			}

			private static void BuildVersion(FunctionBuilder builder) {
				builder.OnExecute(args => Version())
					.ReturnsType(PrimitiveTypes.VarString);
			}

			private static void BuildExists(FunctionBuilder builder) {
				builder.OnExecute(Exists)
					.ReturnsType(PrimitiveTypes.Boolean);
			}

			private static void BuildCoalesce(FunctionBuilder builder) {
				builder.OnExecute(Coalesce)
					.OnReturnType(CoalesceReturnType);
			}

			private static void BuildGreatest(FunctionBuilder builder) {
				builder.OnExecute(Greatest)
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildLeast(FunctionBuilder builder) {
				builder.OnExecute(Least)
					.OnReturnType(FirstArgumentType);
			}

			private static void BuildViewData(FunctionBuilder builder) {
				builder.OnExecute(args => ViewData(args[0], args[1]))
					.ReturnsType(PrimitiveTypes.VarString);
			}

			#endregion

			private static TType FirstArgumentType(ExecuteContext context) {
				return context.Arguments[0].ReturnTType(context);
			}

			private static TType CastReturnType(ExecuteContext context) {
				var typeString = context.EvaluatedArguments[1];
				var encodedType = typeString.Object.ToString();
				return TType.DecodeString(encodedType);
			}

			private static TType CoalesceReturnType(ExecuteContext context) {
				// It's impossible to know the return type of this function until runtime
				// because either comparator could be returned.  We could assume that
				// both branch expressions result in the same type of object but this
				// currently is not enforced.

				// Go through each argument until we find the first parameter we can
				// deduce the class of.
				int count = context.ArgumentCount;
				for (int i = 0; i < count; ++i) {
					TType t = context.Arguments[i].ReturnTType(context.VariableResolver, context.QueryContext);
					if (!(t is TNullType)) {
						return t;
					}
				}

				// Can't work it out so return null type
				return PrimitiveTypes.Null;
			}

			private static TType FRuleConvertReturnType(ExecuteContext context) {
				var argType = context.Arguments[0].ReturnTType(context);
				return argType is TStringType ? (TType)PrimitiveTypes.Numeric : PrimitiveTypes.VarString;
			}

			private static ExecuteResult Exists(ExecuteContext context) {
				return context.FunctionResult(SystemFunctions.Exists(context.EvaluatedArguments[0], context.QueryContext));
			}
		}

		#endregion
	}
}