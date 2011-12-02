// 
//  Copyright 2010  Deveel
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
using System.Globalization;

namespace Deveel.Data.Functions {
	internal class DateFunctionFactory : FunctionFactory {
		public override void Init() {
			AddFunction("dateob", typeof(DateObFunction));
			AddFunction("timeob", typeof(TimeObFunction));
			AddFunction("timestampob", typeof(TimeStampObFunction));
			AddFunction("dateformat", typeof(DateFormatFunction));
			AddFunction("add_months", typeof(AddMonthsFunction));
			AddFunction("months_between", typeof(MonthsBetweenFunction));
			AddFunction("last_day", typeof(LastDayFunction));
			AddFunction("next_day", typeof(NextDayFunction));
			AddFunction("dbtimezone", typeof(DbTimeZoneFunction));
			AddFunction("extract", typeof(ExtractFunction));
			AddFunction("year", typeof(YearFunction));
			AddFunction("month", typeof(MonthFunction));
			AddFunction("day", typeof(DayFunction));
			AddFunction("hour", typeof(HourFunction));
			AddFunction("minute", typeof(MinuteFunction));
			AddFunction("second", typeof(SecondFunction));
			AddFunction("intervalob", typeof(IntervalObFunction));
		}

		#region DateObFunction

		[Serializable]
		sealed class DateObFunction : Function {

			private readonly static TType DATE_TYPE = new TDateType(SqlType.Date);

			private static readonly string[] formats = new string[] {
		                                                        	"d-MMM-yy",				// the medium format
		                                                        	"M/dd/yy",				// the short format
		                                                        	"MMM dd%, yyy",			// the long format
		                                                        	"dddd, MMM dd%, yyy",	// the full format
		                                                        	"yyyy-MM-dd"			// the SQL format
		                                                        };


			private static TObject DateVal(DateTime d) {
				return new TObject(DATE_TYPE, d);
			}

			public DateObFunction(Expression[] parameters)
				: base("dateob", parameters) {

				if (ParameterCount > 1) {
					throw new Exception("'dateob' function must have only one or zero parameters.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				// No parameters so return the current date.
				if (ParameterCount == 0) {
					return DateVal(DateTime.Now);
				}

				TObject exp_res = this[0].Evaluate(group, resolver, context);
				// If expression resolves to 'null' then return current date
				if (exp_res.IsNull) {
					return DateVal(DateTime.Now);
				}
				// If expression resolves to a BigDecimal, then treat as number of
				// seconds since midnight Jan 1st, 1970
				if (exp_res.TType is TNumericType) {
					BigNumber num = (BigNumber) exp_res.Object;
					return DateVal(new DateTime(num.ToInt64()));
				}

				string date_str = exp_res.Object.ToString();

				// Try and parse date
				try {
					return DateVal(DateTime.ParseExact(date_str, formats, CultureInfo.CurrentCulture, DateTimeStyles.None));
				} catch {
					throw new Exception("Unable to parse date string '" + date_str + "'");
				}
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return DATE_TYPE;
			}
		}

		#endregion

		#region TimeObFunction

		[Serializable]
		sealed class TimeObFunction : Function {

			private readonly static TType TIME_TYPE = new TDateType(SqlType.Time);

			public TimeObFunction(Expression[] parameters)
				: base("timeob", parameters) {

				if (ParameterCount > 1) {
					throw new Exception(
						"'timeob' function must have only one or zero parameters.");
				}
			}

			private static TObject timeNow() {
				return new TObject(TIME_TYPE, DateTime.Now);
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
											 IQueryContext context) {

				// No parameters so return the current time.
				if (ParameterCount == 0) {
					return timeNow();
				}

				TObject exp_res = this[0].Evaluate(group, resolver, context);
				// If expression resolves to 'null' then return current date
				if (exp_res.IsNull) {
					return timeNow();
				}

				String date_str = exp_res.Object.ToString();

				return new TObject(TIME_TYPE, CastHelper.ToTime(date_str));

			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TIME_TYPE;
			}
		}

		#endregion

		#region TimeStampObFunction

		[Serializable]
		class TimeStampObFunction : Function {

			private readonly static TType TIMESTAMP_TYPE = new TDateType(SqlType.TimeStamp);

			public TimeStampObFunction(Expression[] parameters)
				: base("timestampob", parameters) {

				if (ParameterCount > 1)
					throw new Exception("'timestampob' function must have only one or zero parameters.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
											 IQueryContext context) {

				// No parameters so return the current time.
				if (ParameterCount == 0) {
					return new TObject(TIMESTAMP_TYPE, DateTime.Now);
				}

				TObject exp_res = this[0].Evaluate(group, resolver, context);
				// If expression resolves to 'null' then return current date
				if (exp_res.IsNull) {
					return new TObject(TIMESTAMP_TYPE, DateTime.Now);
				}

				String date_str = exp_res.Object.ToString();

				return new TObject(TIMESTAMP_TYPE, CastHelper.ToTimeStamp(date_str));

			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TIMESTAMP_TYPE;
			}
		}

		#endregion

		#region DateFormatFunction

		// A function that formats an input DateTime object to the format
		// given using the string format.
		[Serializable]
		sealed class DateFormatFunction : Function {
			public DateFormatFunction(Expression[] parameters)
				: base("dateformat", parameters) {

				if (ParameterCount != 2)
					throw new Exception("'dateformat' function must have exactly two parameters.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject datein = this[0].Evaluate(group, resolver, context);
				TObject format = this[1].Evaluate(group, resolver, context);
				// If expression resolves to 'null' then return null
				if (datein.IsNull) {
					return datein;
				}

				DateTime d;
				if (!(datein.TType is TDateType)) {
					throw new Exception("Date to format must be DATE, TIME or TIMESTAMP");
				} else {
					d = (DateTime)datein.Object;
				}

				String format_string = format.ToString();
				return TObject.CreateString(d.ToString(format_string));
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TType.StringType;
			}
		}


		#endregion

		#region AddMonthsFunction

		[Serializable]
		private class AddMonthsFunction : Function {
			public AddMonthsFunction(Expression[] parameters)
				: base("add_months", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob1 = this[0].Evaluate(group, resolver, context);

				if (ob1.IsNull || !(ob1.TType is TDateType))
					return ob1;

				TObject ob2 = this[1].Evaluate(group, resolver, context);
				if (ob2.IsNull)
					return ob1;

				DateTime date = ob1.ToDateTime();
				int value = ob2.ToBigNumber().ToInt32();

				return TObject.CreateDateTime(date.AddMonths(value));
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TType.DateType;
			}
		}

		#endregion

		#region MonthsBetweenFunction

		[Serializable]
		private class MonthsBetweenFunction : Function {
			public MonthsBetweenFunction(Expression[] parameters)
				: base("months_between", parameters) {
				if (ParameterCount != 2)
					throw new ArgumentException("The MONTHS_BETWEEN function requires exactly 2 parameters.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob1 = this[0].Evaluate(group, resolver, context);
				TObject ob2 = this[1].Evaluate(group, resolver, context);

				if (ob1.IsNull || ob2.IsNull)
					return TObject.Null;

				DateTime date1 = ob1.ToDateTime();
				DateTime date2 = ob2.ToDateTime();

				Interval span = new Interval(date1, date2);
				return TObject.CreateInt4(span.Months);
			}
		}

		#endregion

		#region LastDayFunction

		[Serializable]
		private class LastDayFunction : Function {
			public LastDayFunction(Expression[] parameters)
				: base("last_day", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				DateTime date = ob.ToDateTime();

				DateTime evalDate = new DateTime(date.Year, date.Month, 1);
				evalDate = evalDate.AddMonths(1).Subtract(new TimeSpan(1, 0, 0, 0, 0));

				return TObject.CreateDateTime(evalDate);
			}
		}

		#endregion

		#region NextDayFunction

		[Serializable]
		private class NextDayFunction : Function {
			public NextDayFunction(Expression[] parameters) 
				: base("next_day", parameters) {
				if (ParameterCount != 2)
					throw new ArgumentException("The function NET_DAY requires exactly 2 parameters.");
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

			private static DayOfWeek GetDayOfWeek(TObject ob) {
				if (ob.TType is TNumericType)
					return (DayOfWeek) ob.ToBigNumber().ToInt32();
				return (DayOfWeek) Enum.Parse(typeof (DayOfWeek), ob.ToStringValue(), true);
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob1 = this[0].Evaluate(group, resolver, context);
				TObject ob2 = this[1].Evaluate(group, resolver, context);

				if (ob1.IsNull || ob2.IsNull)
					return TObject.Null;

				DateTime date = ob1.ToDateTime();
				DateTime nextDate = GetNextDateForDay(date, GetDayOfWeek(ob2));

				return TObject.CreateDateTime(nextDate);
			}
		}

		#endregion

		#region DbTimeZoneFunction

		[Serializable]
		private class DbTimeZoneFunction : Function {
			public DbTimeZoneFunction(Expression[] parameters) 
				: base("dbtimezone", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				return TObject.CreateString(TimeZone.CurrentTimeZone.StandardName);
			}
		}

		#endregion

		#region ExtractFunction

		[Serializable]
		private class ExtractFunction : Function {
			public ExtractFunction(Expression[] parameters) 
				: base("extract", parameters) {
			}

			internal static int ExtractField(string field, TObject obj) {
				DateTime dateTime = DateTime.MinValue;
				Interval timeSpan = Interval.Zero;
				bool fromTs = false;

				if (obj.TType is TDateType) {
					dateTime = obj.ToDateTime();
				} else if (obj.TType is TIntervalType) {
					timeSpan = obj.ToInterval();
					fromTs = true;
				} else {
					obj = obj.CastTo(TType.DateType);
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

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject field = this[0].Evaluate(group, resolver, context);
				TObject date = this[1].Evaluate(group, resolver, context);

				if (field.IsNull)
					throw new ArgumentException("The first parameter of EXTRACT function can't be NULL.");

				if (date.IsNull)
					return TObject.Null;

				string field_str = field.ToStringValue();

				return TObject.CreateInt4(ExtractField(field_str, date));
			}
		}

		#endregion

		#region YearFunction

		[Serializable]
		private class YearFunction : Function {
			public YearFunction(Expression[] parameters) 
				: base("year", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				return (TObject) ExtractFunction.ExtractField("year", ob);
			}
		}

		#endregion

		#region MonthFunction

		[Serializable]
		private class MonthFunction : Function {
			public MonthFunction(Expression[] parameters)
				: base("month", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				return (TObject) ExtractFunction.ExtractField("month", ob);
			}
		}

		#endregion

		#region DayFunction

		[Serializable]
		private class DayFunction : Function {
			public DayFunction(Expression[] parameters)
				: base("day", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				return (TObject) ExtractFunction.ExtractField("day", ob);
			}
		}

		#endregion

		#region HourFunction

		[Serializable]
		private class HourFunction : Function {
			public HourFunction(Expression[] parameters)
				: base("hour", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				return (TObject) ExtractFunction.ExtractField("hour", ob);
			}
		}

		#endregion

		#region MinuteFunction

		[Serializable]
		private class MinuteFunction : Function {
			public MinuteFunction(Expression[] parameters)
				: base("minute", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				return (TObject) ExtractFunction.ExtractField("minute", ob);
			}
		}

		#endregion

		#region SecondFunction

		[Serializable]
		private class SecondFunction : Function {
			public SecondFunction(Expression[] parameters)
				: base("second", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				return (TObject) ExtractFunction.ExtractField("second", ob);
			}
		}

		#endregion

		#region IntervalFunction

		[Serializable]
		private class IntervalObFunction : Function {
			public IntervalObFunction(Expression[] parameters) 
				: base("intervalob", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (!(ob.TType is TStringType))
					ob = ob.CastTo(TType.StringType);

				string s = ob.ToStringValue();

				string field = null;
				if (ParameterCount > 1) {
					TObject field_ob = this[1].Evaluate(group, resolver, context);
					if (!field_ob.IsNull)
						field = field_ob.ToStringValue();
				}

				Interval interval = Interval.Zero;
				if (field != null && field.Length > 0) {
					switch(field.ToLower()) {
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

				return (TObject) interval;
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TType.IntervalType;
			}
		}

		#endregion
	}
}