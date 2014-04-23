// 
//  Copyright 2010-2014  Deveel
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
using System.IO;
using System.IO.Compression;
using System.Reflection;
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
	public static class LegacySystemFunctions {
		private static LegacyFunctionFactory factory;

		public static LegacyFunctionFactory Factory {
			get {
				if (factory == null) {
					factory = new SystemFunctionsFactory();
					factory.Init();
				}

				return factory;
			}
		}

		#region SystemFunctionsFactory

		class SystemFunctionsFactory : LegacyFunctionFactory {
			protected override void OnInit() {
				// Arithmetic Functions
				AddFunction("abs", Parameter(0, PrimitiveTypes.Numeric), typeof(AbsInvokedFunction));
				AddFunction("acos", typeof(ACosInvokedFunction));
				AddFunction("asin", typeof(ASinInvokedFunction));
				AddFunction("atan", typeof(ATanInvokedFunction));
				AddFunction("cos", typeof(CosInvokedFunction));
				AddFunction("cosh", typeof(CosHInvokedFunction));
				AddFunction("sign", typeof(SignInvokedFunction));
				AddFunction("signum", typeof(SignInvokedFunction));
				AddFunction("sin", typeof(SinInvokedFunction));
				AddFunction("sinh", typeof(SinHInvokedFunction));
				AddFunction("sqrt", typeof(SqrtInvokedFunction));
				AddFunction("tan", typeof(TanInvokedFunction));
				AddFunction("tanh", typeof(TanHInvokedFunction));
				AddFunction("mod", typeof(ModInvokedFunction));
				AddFunction("pow", typeof(PowInvokedFunction));
				AddFunction("round", typeof(RoundInvokedFunction));
				AddFunction("log", typeof(LogInvokedFunction));
				AddFunction("log10", typeof(Log10InvokedFunction));
				AddFunction("pi", typeof(PiInvokedFunction));
				AddFunction("e", typeof(EInvokedFunction));
				AddFunction("ceil", typeof(CeilInvokedFunction));
				AddFunction("ceiling", typeof(CeilInvokedFunction));
				AddFunction("floor", typeof(FloorInvokedFunction));
				AddFunction("radians", typeof(RadiansInvokedFunction));
				AddFunction("degrees", typeof(DegreesInvokedFunction));
				AddFunction("exp", typeof(ExpInvokedFunction));
				AddFunction("cot", typeof(CotInvokedFunction));
				AddFunction("arctan", typeof(ArcTanInvokedFunction));
				AddFunction("rand", typeof(RandInvokedFunction));

				// Date/Time Functions
				AddFunction("dateob", typeof(DateObInvokedFunction));
				AddFunction("timeob", typeof(TimeObInvokedFunction));
				AddFunction("timestampob", typeof(TimeStampObInvokedFunction));
				AddFunction("dateformat", typeof(DateFormatInvokedFunction));
				AddFunction("add_months", typeof(AddMonthsInvokedFunction));
				AddFunction("months_between", typeof(MonthsBetweenInvokedFunction));
				AddFunction("last_day", typeof(LastDayInvokedFunction));
				AddFunction("next_day", typeof(NextDayInvokedFunction));
				AddFunction("dbtimezone", typeof(DbTimeZoneInvokedFunction));
				AddFunction("extract", typeof(ExtractInvokedFunction));
				AddFunction("year", typeof(YearInvokedFunction));
				AddFunction("month", typeof(MonthInvokedFunction));
				AddFunction("day", typeof(DayInvokedFunction));
				AddFunction("hour", typeof(HourInvokedFunction));
				AddFunction("minute", typeof(MinuteInvokedFunction));
				AddFunction("second", typeof(SecondInvokedFunction));
				AddFunction("intervalob", typeof(IntervalObInvokedFunction));

				// String Functions
				AddFunction("substring", typeof(SubstringInvokedFunction));

				// Binary Functions
				AddFunction("crc32", typeof(Crc32InvokedFunction));
				AddFunction("adler32", typeof(Adler32InvokedFunction));
				AddFunction("compress", typeof(CompressInvokedFunction));
				AddFunction("uncompress", typeof(UncompressInvokedFunction));
				AddFunction("octet_length", typeof(OctetLengthInvokedFunction));

				// Internal Functions
				// Object instantiation (Internal)
				AddFunction("_new_Object", typeof(ObjectInstantiation2));

				// Internal functions
				AddFunction("i_sql_type", typeof(SQLTypeString));
				AddFunction("i_view_data", typeof(ViewDataConvert));
				AddFunction("i_privilege_string", typeof(PrivilegeString));

				// Casting functions
				AddFunction("tonumber", typeof(ToNumberInvokedFunction));
				AddFunction("sql_cast", typeof(SqlCastInvokedFunction));
				// Security
				AddFunction("user", typeof(UserInvokedFunction), FunctionType.StateBased);
				AddFunction("privgroups", typeof(PrivGroupsInvokedFunction), FunctionType.StateBased);
				// Sequence operations
				AddFunction("nextval", typeof(NextValInvokedFunction), FunctionType.StateBased);
				AddFunction("currval", typeof(CurrValInvokedFunction), FunctionType.StateBased);
				AddFunction("setval", typeof(SetValInvokedFunction), FunctionType.StateBased);
				// Misc
				AddFunction("hextobinary", typeof(HexToBinaryInvokedFunction));
				AddFunction("binarytohex", typeof(BinaryToHexInvokedFunction));
				// Lists
				AddFunction("least", typeof(LeastInvokedFunction));
				AddFunction("greatest", typeof(GreatestInvokedFunction));
				// Branch
				AddFunction("if", new[] {Parameter(0, PrimitiveTypes.Boolean), Dynamic(1), Dynamic(2)}, typeof (IfInvokedFunction));
				AddFunction("coalesce", typeof(CoalesceInvokedFunction));

				// identity
				AddFunction("identity", typeof(IdentityInvokedFunction), FunctionType.StateBased);

				AddFunction("version", typeof(VersionInvokedFunction));
				AddFunction("nullif", typeof(NullIfInvokedFunction));
				AddFunction("length", typeof(LengthInvokedFunction));

				AddFunction("sql_exists", typeof(ExistsInvokedFunction));
				AddFunction("sql_unique", typeof(UniqueInvokedFunction));

				// crypto
				AddFunction("hash", typeof(HashInvokedFunction));
			}
		}

		#endregion

		#region AbsFunction

		[Serializable]
		private sealed class AbsInvokedFunction : InvokedFunction {
			public AbsInvokedFunction(Expression[] parameters)
				: base("abs", parameters) {

				if (ParameterCount != 1) {
					throw new Exception("Abs function must have one argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}
				BigNumber num = ob.ToBigNumber();
				return TObject.CreateBigNumber(num.Abs());
			}

		}

		#endregion

		#region ACosFunction

		[Serializable]
		sealed class ACosInvokedFunction : InvokedFunction {
			public ACosInvokedFunction(Expression[] parameters)
				: base("acos", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(System.Math.Acos(ob.ToBigNumber().ToDouble()));
			}
		}

		#endregion

		#region ASinFunction

		[Serializable]
		private sealed class ASinInvokedFunction : InvokedFunction {
			public ASinInvokedFunction(Expression[] parameters)
				: base("asin", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(System.Math.Asin(ob.ToBigNumber().ToDouble()));
			}
		}

		#endregion

		#region ATanFunction

		[Serializable]
		sealed class ATanInvokedFunction : InvokedFunction {
			public ATanInvokedFunction(Expression[] parameters)
				: base("atan", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(System.Math.Atan(ob.ToBigNumber().ToDouble()));
			}
		}


		#endregion

		#region ArcTanFunction

		[Serializable]
		private class ArcTanInvokedFunction : InvokedFunction {
			public ArcTanInvokedFunction(Expression[] parameters)
				: base("arctan", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				double degrees = ob.ToBigNumber().ToDouble();
				double radians = RadiansInvokedFunction.ToRadians(degrees);
				radians = System.Math.Tan(System.Math.Atan(radians));
				degrees = DegreesInvokedFunction.ToDegrees(radians);

				return TObject.CreateBigNumber(degrees);
			}
		}

		#endregion

		#region CotFunction

		[Serializable]
		private class CotInvokedFunction : InvokedFunction {
			public CotInvokedFunction(Expression[] parameters)
				: base("cot", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				double degrees = ob.ToBigNumber().ToDouble();
				double radians = RadiansInvokedFunction.ToRadians(degrees);
				double cotan = 1.0 / System.Math.Tan(radians);

				return TObject.CreateBigNumber(cotan);
			}
		}

		#endregion

		#region CosFunction

		[Serializable]
		sealed class CosInvokedFunction : InvokedFunction {
			public CosInvokedFunction(Expression[] parameters)
				: base("cos", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(System.Math.Cos(ob.ToBigNumber().ToDouble()));
			}
		}


		#endregion

		#region CosHFunction

		[Serializable]
		sealed class CosHInvokedFunction : InvokedFunction {
			public CosHInvokedFunction(Expression[] parameters)
				: base("cosh", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(System.Math.Cosh(ob.ToBigNumber().ToDouble()));
			}
		}


		#endregion

		#region SignFunction

		[Serializable]
		sealed class SignInvokedFunction : InvokedFunction {
			public SignInvokedFunction(Expression[] parameters)
				: base("sign", parameters) {

				if (ParameterCount != 1) {
					throw new Exception("Sign function must have one argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}
				BigNumber num = ob.ToBigNumber();
				return TObject.CreateInt4(num.Signum());
			}
		}

		#endregion

		#region SinFunction

		[Serializable]
		class SinInvokedFunction : InvokedFunction {
			public SinInvokedFunction(Expression[] parameters)
				: base("sin", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(System.Math.Sin(ob.ToBigNumber().ToDouble()));
			}
		}

		#endregion

		#region SinHFunction

		[Serializable]
		class SinHInvokedFunction : InvokedFunction {
			public SinHInvokedFunction(Expression[] parameters)
				: base("sinh", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(System.Math.Sinh(ob.ToBigNumber().ToDouble()));
			}
		}

		#endregion

		#region CeilFunction

		[Serializable]
		private class CeilInvokedFunction : InvokedFunction {
			public CeilInvokedFunction(Expression[] parameters)
				: base("ceil", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				if (!(ob.TType is TNumericType))
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(ob.ToBigNumber().ToDouble());
			}
		}

		#endregion

		#region FloorFunction

		[Serializable]
		private class FloorInvokedFunction : InvokedFunction {
			public FloorInvokedFunction(Expression[] parameters)
				: base("floor", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				if (!(ob.TType is TNumericType))
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(System.Math.Floor(ob.ToBigNumber().ToDouble()));
			}
		}

		#endregion

		#region RadiansFunction

		[Serializable]
		private class RadiansInvokedFunction : InvokedFunction {
			public RadiansInvokedFunction(Expression[] parameters)
				: base("radians", parameters) {
			}

			/// <summary>
			/// The number of radians for one degree.
			/// </summary>
			private const double Degree = 0.0174532925;

			internal static double ToRadians(double degrees) {
				return degrees * Degree;
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				double degrees = ob.ToBigNumber().ToDouble();
				double radians = ToRadians(degrees);

				return TObject.CreateBigNumber(radians);
			}
		}

		#endregion

		#region DegreesFunction

		[Serializable]
		private class DegreesInvokedFunction : InvokedFunction {
			public DegreesInvokedFunction(Expression[] parameters)
				: base("degrees", parameters) {
			}

			/// <summary>
			/// The number of degrees for one radiant.
			/// </summary>
			private const double Radiant = 57.2957795;

			internal static double ToDegrees(double radians) {
				return radians * Radiant;
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				double radians = ob.ToBigNumber().ToDouble();
				double degrees = ToDegrees(radians);

				return TObject.CreateBigNumber(degrees);
			}
		}

		#endregion

		#region ExpFunction

		[Serializable]
		private class ExpInvokedFunction : InvokedFunction {
			public ExpInvokedFunction(Expression[] parameters)
				: base("exp", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				if (!(ob.TType is TNumericType))
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(System.Math.Exp(ob.ToBigNumber().ToDouble()));
			}
		}

		#endregion

		#region RandFunction

		[Serializable]
		private class RandInvokedFunction : InvokedFunction {
			public RandInvokedFunction(Expression[] parameters)
				: base("rand", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				int argc = ParameterCount;

				// TODO: should we initialize at higher level to keep the state?

				Random random;
				if (argc == 1) {
					TObject ob = this[0].Evaluate(group, resolver, context);
					if (!ob.IsNull)
						random = new Random(ob.ToBigNumber().ToInt32());
					else
						random = new Random();
				} else {
					random = new Random();
				}

				double value = random.NextDouble();
				return TObject.CreateBigNumber(value);
			}
		}

		#endregion

		#region DateFormatFunction

		// A function that formats an input DateTime object to the format
		// given using the string format.
		[Serializable]
		sealed class DateFormatInvokedFunction : InvokedFunction {
			public DateFormatInvokedFunction(Expression[] parameters)
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
				return PrimitiveTypes.VarString;
			}
		}


		#endregion

		#region MonthsBetweenFunction

		[Serializable]
		private class MonthsBetweenInvokedFunction : InvokedFunction {
			public MonthsBetweenInvokedFunction(Expression[] parameters)
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
		private class LastDayInvokedFunction : InvokedFunction {
			public LastDayInvokedFunction(Expression[] parameters)
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

		#region ExtractFunction

		[Serializable]
		private class ExtractInvokedFunction : InvokedFunction {
			public ExtractInvokedFunction(Expression[] parameters)
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
		private class YearInvokedFunction : InvokedFunction {
			public YearInvokedFunction(Expression[] parameters)
				: base("year", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				return (TObject)ExtractInvokedFunction.ExtractField("year", ob);
			}
		}

		#endregion

		#region MonthFunction

		[Serializable]
		private class MonthInvokedFunction : InvokedFunction {
			public MonthInvokedFunction(Expression[] parameters)
				: base("month", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				return (TObject)ExtractInvokedFunction.ExtractField("month", ob);
			}
		}

		#endregion

		#region DayFunction

		[Serializable]
		private class DayInvokedFunction : InvokedFunction {
			public DayInvokedFunction(Expression[] parameters)
				: base("day", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				return (TObject)ExtractInvokedFunction.ExtractField("day", ob);
			}
		}

		#endregion

		#region HourFunction

		[Serializable]
		private class HourInvokedFunction : InvokedFunction {
			public HourInvokedFunction(Expression[] parameters)
				: base("hour", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				return (TObject)ExtractInvokedFunction.ExtractField("hour", ob);
			}
		}

		#endregion

		#region MinuteFunction

		[Serializable]
		private class MinuteInvokedFunction : InvokedFunction {
			public MinuteInvokedFunction(Expression[] parameters)
				: base("minute", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				return (TObject)ExtractInvokedFunction.ExtractField("minute", ob);
			}
		}

		#endregion

		#region SecondFunction

		[Serializable]
		private class SecondInvokedFunction : InvokedFunction {
			public SecondInvokedFunction(Expression[] parameters)
				: base("second", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				return (TObject)ExtractInvokedFunction.ExtractField("second", ob);
			}
		}

		#endregion

		#region IntervalFunction

		[Serializable]
		private class IntervalObInvokedFunction : InvokedFunction {
			public IntervalObInvokedFunction(Expression[] parameters)
				: base("intervalob", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (!(ob.TType is TStringType))
					ob = ob.CastTo(PrimitiveTypes.VarString);

				string s = ob.ToStringValue();

				string field = null;
				if (ParameterCount > 1) {
					TObject field_ob = this[1].Evaluate(group, resolver, context);
					if (!field_ob.IsNull)
						field = field_ob.ToStringValue();
				}

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

				return (TObject)interval;
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.IntervalType;
			}
		}

		#endregion

		#region SQLTrimFunction

		#region SubstringFunction

		[Serializable]
		class SubstringInvokedFunction : InvokedFunction {
			public SubstringInvokedFunction(Expression[] parameters)
				: base("substring", parameters) {

				if (ParameterCount < 1 || ParameterCount > 3) {
					throw new Exception("Substring function needs one to three arguments.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}
				String str = ob.Object.ToString();
				int pcount = ParameterCount;
				int str_length = str.Length;
				int arg1 = 1;
				int arg2 = str_length;
				if (pcount >= 2) {
					arg1 = this[1].Evaluate(group, resolver, context).ToBigNumber().ToInt32();
				}
				if (pcount >= 3) {
					arg2 = this[2].Evaluate(group, resolver, context).ToBigNumber().ToInt32();
				}

				// Make sure this call is safe for all lengths of string.
				if (arg1 < 1) {
					arg1 = 1;
				}
				if (arg1 > str_length) {
					return TObject.CreateString("");
				}
				if (arg2 + arg1 > str_length) {
					arg2 = (str_length - arg1) + 1;
				}
				if (arg2 < 1) {
					return TObject.CreateString("");
				}

				//TODO: check this...
				return TObject.CreateString(str.Substring(arg1 - 1, (arg1 + arg2) - 1));
			}

			protected override TType ReturnTType() {
				return PrimitiveTypes.VarString;
			}

		}

		#endregion

		#region InStrFunction

		[Serializable]
		private class InStrInvokedFunction : InvokedFunction {
			public InStrInvokedFunction(Expression[] parameters)
				: base("instr", parameters) {
				if (ParameterCount < 2 || ParameterCount > 4)
					throw new ArgumentException("The function INSTR must specify at least 2 and less than 4 parameters.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				int argc = ParameterCount;

				TObject ob1 = this[0].Evaluate(group, resolver, context);
				TObject ob2 = this[1].Evaluate(group, resolver, context);

				if (ob1.IsNull)
					return TObject.Null;

				if (ob2.IsNull)
					return TObject.CreateInt4(-1);

				string str = ob1.Object.ToString();
				string pattern = ob2.Object.ToString();

				if (str.Length == 0 || pattern.Length == 0)
					return TObject.CreateInt4(-1);

				int startIndex = -1;
				int endIndex = -1;

				if (argc > 2) {
					TObject ob3 = this[2].Evaluate(group, resolver, context);
					if (!ob3.IsNull)
						startIndex = ob3.ToBigNumber().ToInt32();
				}
				if (argc > 3) {
					TObject ob4 = this[3].Evaluate(group, resolver, context);
					if (!ob4.IsNull)
						endIndex = ob4.ToBigNumber().ToInt32();
				}

				int index = -1;
				if (argc == 2) {
					index = str.IndexOf(pattern);
				} else if (argc == 3) {
					index = str.IndexOf(pattern, startIndex);
				} else {
					index = str.IndexOf(pattern, startIndex, endIndex - startIndex);
				}

				return TObject.CreateInt4(index);
			}
		}

		#endregion

		#region OctetLengthFunction

		[Serializable]
		private class OctetLengthInvokedFunction : InvokedFunction {
			public OctetLengthInvokedFunction(Expression[] parameters)
				: base("octet_length", parameters) {
			}

			#region Overrides of Function

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (!(ob.TType is TStringType) || ob.IsNull)
					return TObject.Null;

				IStringAccessor s = (IStringAccessor)ob.Object;
				if (s == null)
					return TObject.Null;

				// by default a character is an UNICODE, which requires 
				// two bytes...
				long size = s.Length * 2;
				if (s is IRef)
					size = (s as IRef).RawSize;

				return (TObject)size;
			}

			#endregion
		}

		#endregion

		#region SQLTypeString

		// Used to form an SQL type string that describes the SQL type and any
		// size/scale information together with it.
		[Serializable]
		class SQLTypeString : InvokedFunction {
			public SQLTypeString(Expression[] parameters)
				: base("i_sql_type", parameters) {

				if (ParameterCount != 3)
					throw new Exception("i_sql_type function must have three arguments.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				// The parameter should be a variable reference that is resolved
				TObject type_string = this[0].Evaluate(group, resolver, context);
				TObject type_size = this[1].Evaluate(group, resolver, context);
				TObject type_scale = this[2].Evaluate(group, resolver, context);

				StringBuilder result_str = new StringBuilder();
				result_str.Append(type_string.ToString());
				long size = -1;
				long scale = -1;
				if (!type_size.IsNull) {
					size = type_size.ToBigNumber().ToInt64();
				}
				if (!type_scale.IsNull) {
					scale = type_scale.ToBigNumber().ToInt64();
				}

				if (size != -1) {
					result_str.Append('(');
					result_str.Append(size);
					if (scale != -1) {
						result_str.Append(',');
						result_str.Append(scale);
					}
					result_str.Append(')');
				}

				return TObject.CreateString(result_str.ToString());
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.VarString;
			}

		}

		#endregion

		#region ObjectInstantiation

		// Instantiates a new object.
		[Serializable]
		class ObjectInstantiation : InvokedFunction {
			public ObjectInstantiation(Expression[] parameters)
				: base("_new_Object", parameters) {

				if (ParameterCount < 1) {
					throw new Exception("_new_Object function must have one argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				// Resolve the parameters...
				int arg_len = ParameterCount - 1;
				Object[] args = new Object[arg_len];
				for (int i = 0; i < args.Length; ++i) {
					args[i] = this[i + 1].Evaluate(group, resolver,
												   context).Object;
				}
				Object[] casted_args = new Object[arg_len];

				try {
					String typeName = this[0].Evaluate(null, resolver, context).Object.ToString();
					Type c = Type.GetType(typeName);

					ConstructorInfo[] constructs = c.GetConstructors();
					// Search for the first constructor that we can use with the given
					// arguments.
					// search_constructs:
					for (int i = 0; i < constructs.Length; ++i) {
						ParameterInfo[] construct_args = constructs[i].GetParameters();
						if (construct_args.Length == arg_len) {
							for (int n = 0; n < arg_len; ++n) {
								// If we are dealing with a primitive,
								if (construct_args[n].ParameterType.IsPrimitive) {
									String class_name = construct_args[n].ParameterType.Name;
									// If the given argument is a number,
									if (Caster.IsNumber(args[n])) {
										if (class_name.Equals("byte")) {
											casted_args[n] = Convert.ToByte(args[n]);
										} else if (class_name.Equals("char")) {
											casted_args[n] = Convert.ToChar((int)args[n]);
										} else if (class_name.Equals("double")) {
											casted_args[n] = Convert.ToDouble(args[n]);
										} else if (class_name.Equals("float")) {
											casted_args[n] = Convert.ToSingle(args[n]);
										} else if (class_name.Equals("int")) {
											casted_args[n] = Convert.ToInt32(args[n]);
										} else if (class_name.Equals("long")) {
											casted_args[n] = Convert.ToInt64(args[n]);
										} else if (class_name.Equals("short")) {
											casted_args[n] = Convert.ToInt16(args[n]);
										} else {
											// Can't cast the primitive type to a number so break,
											// break search_constructs;
											break;
										}

									}
										// If we are a bool, we can cast to primitive bool
									else if (args[n] is Boolean) {
										// If primitive type constructor arg is a bool also
										if (class_name.Equals("bool")) {
											casted_args[n] = args[n];
										} else {
											// break search_constructs;
											break;
										}
									}
										// Otherwise we can't cast,
									else {
										// break search_constructs;
										break;
									}

								}
									// Not a primitive type constructor arg,
								else {
									// PENDING: Allow string -> char conversion
									if (construct_args[n].ParameterType.IsInstanceOfType(args[n])) {
										casted_args[n] = args[n];
									} else {
										// break search_constructs;
										break;
									}
								}
							}  // for (int n = 0; n < arg_len; ++n)
							// If we get here, we have a match...
							Object ob = constructs[i].Invoke(casted_args);
							ByteLongObject serialized_ob = ObjectTranslator.Serialize(ob);
							return new TObject(new TObjectType(typeName), serialized_ob);
						}
					}

					throw new Exception(
						"Unable to find a constructor for '" + typeName +
						"' that matches given arguments.");

				} catch (TypeLoadException e) {
					throw new Exception("Type not found: " + e.Message);
				} catch (TypeInitializationException e) {
					throw new Exception("Instantiation ApplicationException: " + e.Message);
				} catch (AccessViolationException e) {
					throw new Exception("Illegal Access ApplicationException: " + e.Message);
				} catch (ArgumentException e) {
					throw new Exception("Illegal Argument ApplicationException: " + e.Message);
				} catch (TargetInvocationException e) {
					throw new Exception("Invocation Target ApplicationException: " + e.Message);
				}

			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				String clazz = this[0].Evaluate(null, resolver,
												context).Object.ToString();
				return new TObjectType(clazz);
			}

		}

		#endregion

		#region ObjectInstantiation2

		[Serializable]
		class ObjectInstantiation2 : InvokedFunction {
			public ObjectInstantiation2(Expression[] parameters)
				: base("_new_Object", parameters) {

				if (ParameterCount < 1) {
					throw new Exception("_new_Object function must have one argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				// Resolve the parameters...
				int arg_len = ParameterCount - 1;
				TObject[] args = new TObject[arg_len];
				for (int i = 0; i < args.Length; ++i) {
					args[i] = this[i + 1].Evaluate(group, resolver, context);
				}
				Caster.DeserializeObjects(args);

				try {
					// Get the class name of the object to be constructed
					String clazz = this[0].Evaluate(null, resolver,
													context).Object.ToString();
					Type c = Type.GetType(clazz);
					ConstructorInfo[] constructs = c.GetConstructors();

					ConstructorInfo bestConstructor =
						Caster.FindBestConstructor(constructs, args);
					if (bestConstructor == null) {
						// Didn't find a match - build a list of class names of the
						// args so the user knows what we were looking for.
						String argTypes = Caster.GetArgTypesString(args);
						throw new Exception(
							"Unable to find a constructor for '" + clazz +
							"' that matches given arguments: " + argTypes);
					}
					Object[] casted_args =
						Caster.CastArgsToConstructor(args, bestConstructor);
					// Call the constructor to create the object
					Object ob = bestConstructor.Invoke(casted_args);
					ByteLongObject serialized_ob = ObjectTranslator.Serialize(ob);
					return new TObject(new TObjectType(clazz), serialized_ob);

				} catch (TypeLoadException e) {
					throw new Exception("Class not found: " + e.Message);
				} catch (TypeInitializationException e) {
					throw new Exception("Instantiation ApplicationException: " + e.Message);
				} catch (AccessViolationException e) {
					throw new Exception("Illegal Access ApplicationException: " + e.Message);
				} catch (ArgumentException e) {
					throw new Exception("Illegal Argument ApplicationException: " + e.Message);
				} catch (TargetInvocationException e) {
					String msg = e.Message;
					if (msg == null) {
						Exception th = e.InnerException;
						if (th != null) {
							msg = th.GetType().Name + ": " + th.Message;
						}
					}
					throw new Exception("Invocation Target ApplicationException: " + msg);
				}

			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				String clazz = this[0].Evaluate(null, resolver,
												context).Object.ToString();
				return new TObjectType(clazz);
			}

		}

		#endregion

		#region PrivilegeString

		// Given a priv_bit number (from SYSTEM.grant), this will return a
		// text representation of the privilege.
		[Serializable]
		class PrivilegeString : InvokedFunction {

			public PrivilegeString(Expression[] parameters)
				: base("i_privilege_string", parameters) {

				if (ParameterCount != 1) {
					throw new Exception(
						"i_privilege_string function must have one argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
											 IQueryContext context) {
				TObject priv_bit_ob = this[0].Evaluate(group, resolver, context);
				int priv_bit = ((BigNumber)priv_bit_ob.Object).ToInt32();
				Privileges privs = new Privileges();
				privs = privs.Add(priv_bit);
				return TObject.CreateString(privs.ToString());
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.VarString;
			}
		}

		#endregion

		#region Crc32Function

		[Serializable]
		private class Crc32InvokedFunction : InvokedFunction {
			public Crc32InvokedFunction(Expression[] parameters)
				: base("crc32", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				//TODO: needs some revision...
				MemoryStream stream;
				if (ob.TType is TStringType) {
					IStringAccessor str = (IStringAccessor)ob.Object;
					TextReader reader = str.GetTextReader();
					stream = new MemoryStream(str.Length);
					ReadIntoStream(reader, stream);
				} else if (ob.TType is TBinaryType) {
					IBlobAccessor blob = (IBlobAccessor)ob.Object;
					stream = new MemoryStream(blob.Length);
					CopyStream(blob.GetInputStream(), stream);
				} else {
					ob = ob.CastTo(PrimitiveTypes.VarString);
					StringObject str = StringObject.FromString(ob.ToStringValue());
					TextReader reader = str.GetTextReader();
					stream = new MemoryStream(str.Length);
					ReadIntoStream(reader, stream);
				}

				Crc32 crc32 = new Crc32();
				crc32.ComputeHash(stream);

				return TObject.CreateBigNumber(crc32.CrcValue);
			}

		}

		#endregion

		#region Adler32Function

		[Serializable]
		private class Adler32InvokedFunction : InvokedFunction {
			public Adler32InvokedFunction(Expression[] parameters)
				: base("adler32", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				//TODO: needs some revision...
				MemoryStream stream;
				if (ob.TType is TStringType) {
					IStringAccessor str = (IStringAccessor)ob.Object;
					TextReader reader = str.GetTextReader();
					stream = new MemoryStream(str.Length);
					ReadIntoStream(reader, stream);
				} else if (ob.TType is TBinaryType) {
					IBlobAccessor blob = (IBlobAccessor)ob.Object;
					stream = new MemoryStream(blob.Length);
					CopyStream(blob.GetInputStream(), stream);
				} else {
					ob = ob.CastTo(PrimitiveTypes.VarString);
					StringObject str = StringObject.FromString(ob.ToStringValue());
					TextReader reader = str.GetTextReader();
					stream = new MemoryStream(str.Length);
					ReadIntoStream(reader, stream);
				}

				Adler32 adler32 = new Adler32();
				byte[] result = adler32.ComputeHash(stream);
				return TObject.CreateBigNumber(BitConverter.ToInt32(result, 0));
			}
		}

		#endregion

		#region CompressFunction

		[Serializable]
		private class CompressInvokedFunction : InvokedFunction {
			public CompressInvokedFunction(Expression[] parameters)
				: base("compress", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				MemoryStream stream;
				if (ob.TType is TStringType) {
					IStringAccessor str = (IStringAccessor)ob.Object;
					TextReader reader = str.GetTextReader();
					stream = new MemoryStream(str.Length);
					ReadIntoStream(reader, stream);
				} else if (ob.TType is TBinaryType) {
					IBlobAccessor blob = (IBlobAccessor)ob.Object;
					stream = new MemoryStream(blob.Length);
					CopyStream(blob.GetInputStream(), stream);
				} else {
					ob = ob.CastTo(PrimitiveTypes.VarString);
					StringObject str = StringObject.FromString(ob.ToStringValue());
					TextReader reader = str.GetTextReader();
					stream = new MemoryStream(str.Length);
					ReadIntoStream(reader, stream);
				}

				MemoryStream tempStream = new MemoryStream();
				DeflateStream outputStream = new DeflateStream(tempStream, CompressionMode.Compress);

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

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.BinaryType;
			}
		}

		#endregion

		#region UncompressFunction

		[Serializable]
		private class UncompressInvokedFunction : InvokedFunction {
			public UncompressInvokedFunction(Expression[] parameters)
				: base("uncompress", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				MemoryStream stream;
				if (ob.TType is TStringType) {
					IStringAccessor str = (IStringAccessor)ob.Object;
					TextReader reader = str.GetTextReader();
					stream = new MemoryStream(str.Length);
					ReadIntoStream(reader, stream);
				} else if (ob.TType is TBinaryType) {
					IBlobAccessor blob = (IBlobAccessor)ob.Object;
					stream = new MemoryStream(blob.Length);
					CopyStream(blob.GetInputStream(), stream);
				} else {
					ob = ob.CastTo(PrimitiveTypes.VarString);
					StringObject str = StringObject.FromString(ob.ToStringValue());
					TextReader reader = str.GetTextReader();
					stream = new MemoryStream(str.Length);
					ReadIntoStream(reader, stream);
				}

				MemoryStream tmpStream = new MemoryStream();
				DeflateStream inputStream = new DeflateStream(stream, CompressionMode.Decompress);

				const int bufferSize = 1024;
				byte[] buffer = new byte[bufferSize];

				int bytesRead;
				while ((bytesRead = inputStream.Read(buffer, 0, bufferSize)) != 0) {
					tmpStream.Write(buffer, 0, bytesRead);
				}

				byte[] output = tmpStream.ToArray();
				return new TObject(PrimitiveTypes.BinaryType, output);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.BinaryType;
			}
		}

		#endregion

		#region Adler32

		private class Adler32 : HashAlgorithm {
			public Adler32()
				: base() {
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
					sum_1 = (ushort)((sum_1 + array[i]) % 65521);
					sum_2 = (ushort)((sum_1 + sum_2) % 65521);
				}
			}

			protected override byte[] HashFinal() {
				// concat the two 16 bit values to form
				// one 32-bit value
				uint value = (uint)((sum_2 << 16) | sum_1);
				// use the bitconverter class to render the
				// 32-bit integer into an array of bytes
				return BitConverter.GetBytes(value);
			}

			#endregion
		}

		#endregion

		#region Crc32

		private class Crc32 : HashAlgorithm {
			public const uint DefaultSeed = 0xffffffff;

			readonly static uint[] CrcTable = new uint[] {
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

			uint crcValue = 0;

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
				this.HashValue = new byte[] { (byte)((crcValue >> 24) & 0xff), 
                                      (byte)((crcValue >> 16) & 0xff), 
                                      (byte)((crcValue >> 8) & 0xff), 
                                      (byte)(crcValue & 0xff) };
				return this.HashValue;
			}
			public uint CrcValue {
				get {
					return (uint)((HashValue[0] << 24) | (HashValue[1] << 16) | (HashValue[2] << 8) | HashValue[3]);
				}
			}
			public override int HashSize {
				get { return 32; }
			}
		}

		#endregion

		private static void ReadIntoStream(TextReader reader, Stream stream) {
			string line;
			while ((line = reader.ReadLine()) != null) {
				byte[] buffer = Encoding.Unicode.GetBytes(line);
				stream.Write(buffer, 0, buffer.Length);
			}
		}

		private static void CopyStream(Stream input, Stream output) {
			const int bufferSize = 1024;
			byte[] buffer = new byte[bufferSize];
			int readCount;
			while ((readCount = input.Read(buffer, 0, bufferSize)) != 0) {
				output.Write(buffer, 0, readCount);
			}
		}

		#region ToNumberFunction

		// Casts the expression to a BigDecimal number.  Useful in conjunction with
		// 'dateob'
		[Serializable]
		class ToNumberInvokedFunction : InvokedFunction {
			public ToNumberInvokedFunction(Expression[] parameters)
				: base("tonumber", parameters) {

				if (ParameterCount != 1)
					throw new Exception("TONUMBER function must have one argument.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				// Casts the first parameter to a number
				return this[0].Evaluate(group, resolver, context).CastTo(PrimitiveTypes.Numeric);
			}

		}

		#endregion

		#region IdentityFunction

		sealed class IdentityInvokedFunction : InvokedFunction {
			public IdentityInvokedFunction(Expression[] parameters)
				: base("identity", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject table_name = this[0].Evaluate(group, resolver, context);
				long v = -1;
				try {
					context.CurrentSequenceValue(table_name.ToStringValue());
				} catch (StatementException) {
					if (context is DatabaseQueryContext) {
						v = ((DatabaseQueryContext)context).Connection.CurrentUniqueID(table_name.ToStringValue());
					} else {
						throw new InvalidOperationException();
					}
				}

				if (v == -1)
					throw new InvalidOperationException("Unable to determine the sequence of the table " + table_name);

				return TObject.CreateInt8(v);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.Numeric;
			}
		}


		#endregion

		#region PrivGroupsFunction

		// Returns the comma (",") deliminated priv groups the user belongs to.
		[Serializable]
		class PrivGroupsInvokedFunction : InvokedFunction {
			public PrivGroupsInvokedFunction(Expression[] parameters)
				: base("privgroups", parameters) {

				if (ParameterCount > 0) {
					throw new Exception("'privgroups' function must have no arguments.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				throw new Exception("'PrivGroups' function currently not working.");
			}

			protected override TType ReturnTType() {
				return PrimitiveTypes.VarString;
			}

		}

		#endregion

		#region BinaryToHexFunction

		[Serializable]
		class BinaryToHexInvokedFunction : InvokedFunction {

			readonly static char[] digits = {
		                                	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
		                                	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
		                                	'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
		                                	'u', 'v', 'w', 'x', 'y', 'z'
		                                };

			public BinaryToHexInvokedFunction(Expression[] parameters)
				: base("binarytohex", parameters) {

				// One parameter - our hex string.
				if (ParameterCount != 1) {
					throw new Exception(
						"'binarytohex' function must have only 1 argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
											 IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				} else if (ob.TType is TBinaryType) {
					StringBuilder buf = new StringBuilder();
					IBlobAccessor blob = (IBlobAccessor)ob.Object;
					Stream bin = blob.GetInputStream();
					try {
						int bval = bin.ReadByte();
						while (bval != -1) {
							//TODO: check if this is correct...
							buf.Append(digits[((bval >> 4) & 0x0F)]);
							buf.Append(digits[(bval & 0x0F)]);
							bval = bin.ReadByte();
						}
					} catch (IOException e) {
						Console.Error.WriteLine(e.Message);
						Console.Error.WriteLine(e.StackTrace);
						throw new Exception("IO ApplicationException: " + e.Message);
					}

					return TObject.CreateString(buf.ToString());
				} else {
					throw new Exception("'binarytohex' parameter type is not a binary object.");
				}

			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.VarString;
			}

		}

		#endregion

		#region HexToBinaryFunction

		[Serializable]
		class HexToBinaryInvokedFunction : InvokedFunction {
			public HexToBinaryInvokedFunction(Expression[] parameters)
				: base("hextobinary", parameters) {

				// One parameter - our hex string.
				if (ParameterCount != 1)
					throw new Exception("'hextobinary' function must have only 1 argument.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				String str = this[0].Evaluate(group, resolver, context).Object.ToString();

				int str_len = str.Length;
				if (str_len == 0) {
					return new TObject(PrimitiveTypes.BinaryType, new ByteLongObject(new byte[0]));
				}
				// We translate the string to a byte array,
				byte[] buf = new byte[(str_len + 1) / 2];
				int index = 0;
				if (buf.Length * 2 != str_len) {
					buf[0] = (byte)Char.GetNumericValue(str[0].ToString(), 16);
					++index;
				}
				int v = 0;
				for (int i = index; i < str_len; i += 2) {
					v = ((int)Char.GetNumericValue(str[i].ToString(), 16) << 4) |
						((int)Char.GetNumericValue(str[i + 1].ToString(), 16));
					buf[index] = (byte)(v & 0x0FF);
					++index;
				}

				return new TObject(PrimitiveTypes.BinaryType, new ByteLongObject(buf));
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.BinaryType;
			}

		}

		#endregion

		#region CurrValFunction

		[Serializable]
		class CurrValInvokedFunction : InvokedFunction {
			public CurrValInvokedFunction(Expression[] parameters)
				: base("currval", parameters) {

				// The parameter is the name of the table you want to bring the unique
				// key in from.
				if (ParameterCount != 1) {
					throw new Exception("'currval' function must have only 1 argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				String str = this[0].Evaluate(group, resolver, context).Object.ToString();
				long v = context.CurrentSequenceValue(str);
				return TObject.CreateInt8(v);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.Numeric;
			}
		}

		#endregion

		#region NextValFunction

		[Serializable]
		class NextValInvokedFunction : InvokedFunction {
			public NextValInvokedFunction(Expression[] parameters)
				: base("nextval", parameters) {

				// The parameter is the name of the table you want to bring the unique
				// key in from.
				if (ParameterCount != 1)
					throw new Exception("'nextval' function must have only 1 argument.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				String str = this[0].Evaluate(group, resolver, context).Object.ToString();
				long v = context.NextSequenceValue(str);
				return TObject.CreateInt8(v);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.Numeric;
			}

		}

		#endregion

		#region SetValFunction

		[Serializable]
		class SetValInvokedFunction : InvokedFunction {
			public SetValInvokedFunction(Expression[] parameters)
				: base("setval", parameters) {

				// The parameter is the name of the table you want to bring the unique
				// key in from.
				if (ParameterCount != 2) {
					throw new Exception("'setval' function must have 2 arguments.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				String str = this[0].Evaluate(group, resolver, context).Object.ToString();
				BigNumber num = this[1].Evaluate(group, resolver, context).ToBigNumber();
				long v = num.ToInt64();
				context.SetSequenceValue(str, v);
				return TObject.CreateInt8(v);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.Numeric;
			}
		}

		#endregion

		#region UniqueKeyFunction

		[Serializable]
		class UniqueKeyInvokedFunction : InvokedFunction {
			public UniqueKeyInvokedFunction(Expression[] parameters)
				: base("uniquekey", parameters) {

				// The parameter is the name of the table you want to bring the unique
				// key in from.
				if (ParameterCount != 1) {
					throw new Exception("'uniquekey' function must have only 1 argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				String str = this[0].Evaluate(group, resolver, context).Object.ToString();
				long v = context.NextSequenceValue(str);
				return TObject.CreateInt8(v);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.Numeric;
			}

		}

		#endregion

		#region NullIfFunction

		[Serializable]
		private class NullIfInvokedFunction : InvokedFunction {
			public NullIfInvokedFunction(Expression[] parameters)
				: base("nullif", parameters) {
				if (ParameterCount != 2)
					throw new ArgumentException("The NULLIF function must define exactly 2 parameters.");
			}

			#region Overrides of Function

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob1 = this[0].Evaluate(group, resolver, context);
				TObject ob2 = this[1].Evaluate(group, resolver, context);

				if (ob1.IsNull)
					throw new InvalidOperationException("Cannot compare to a NULL argument.");

				if (!ob1.TType.IsComparableType(ob2.TType))
					throw new InvalidOperationException("The types of the two arguments are not comparable.");

				return ob1.CompareTo(ob2) == 0 ? TObject.Null : ob1;
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				TObject ob1 = this[0].Evaluate(resolver, context);
				return ob1.TType;
			}

			#endregion
		}

		#endregion

		#region UniqueFunction

		[Serializable]
		private class UniqueInvokedFunction : InvokedFunction {
			public UniqueInvokedFunction(Expression[] parameters)
				: base("sql_unique", parameters) {
			}

			#region Overrides of Function

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);

				if (ob.IsNull)
					return TObject.Null;

				if (!(ob.TType is TQueryPlanType))
					throw new ArgumentException("The function UNIQUE must be evaluated against a query.");

				IQueryPlanNode plan = (IQueryPlanNode)ob.Object;
				if (plan == null)
					return TObject.Null;

				Table table = plan.Evaluate(context);

				throw new NotImplementedException();
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.Boolean;
			}

			#endregion
		}

		#endregion

		#region HashFunction

		class HashInvokedFunction : InvokedFunction {
			public HashInvokedFunction(Expression[] parameters)
				: base("hash", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				var functionName = this[0].Evaluate(group, resolver, context);

				if (functionName.IsNull)
					throw new InvalidOperationException("Hash function name required.");

				var hash = HashFunctions.GetFunction((string)functionName.Object);
				if (hash == null)
					throw new NotSupportedException(String.Format("Hash function {0} is not supported by the system.", functionName));

				var data = this[1].Evaluate(group, resolver, context);

				if (data.TType is TBinaryType) {
					var str = data.ToStringValue();
					var result = hash.ComputeString(str);
					return TObject.CreateString(result);
				}
				if (data.TType is TStringType) {
					var blob = (ByteLongObject)data.Object;
					var result = hash.Compute(blob.ToArray());
					return new TObject(TType.GetBinaryType(SqlType.Binary, result.Length), result);
				}

				throw new InvalidOperationException("Data type argument not supported");
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return this[1].ReturnTType(resolver, context);
			}
		}

		#endregion
	}
}