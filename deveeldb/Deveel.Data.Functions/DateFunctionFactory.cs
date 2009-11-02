using System;
using System.Globalization;

using Deveel.Math;

namespace Deveel.Data.Functions {
	internal class DateFunctionFactory : FunctionFactory {
		public override void Init() {
			AddFunction("dateob", typeof(DateObFunction));
			AddFunction("timeob", typeof(TimeObFunction));
			AddFunction("timestampob", typeof(TimeStampObFunction));
			AddFunction("dateformat", typeof(DateFormatFunction));
		}

		#region DateObFunction

		[Serializable]
		sealed class DateObFunction : Function {

			private readonly static TType DATE_TYPE = new TDateType(SQLTypes.DATE);

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
				else if (exp_res.TType is TNumericType) {
					BigNumber num = (BigNumber)exp_res.Object;
					return DateVal(new DateTime(num.ToInt64()));
				}

				String date_str = exp_res.Object.ToString();

				// We need to synchronize here unfortunately because the Java
				// DateFormat objects are not thread-safe.
				lock (formats) {
					// Try and parse date
					try {
						return DateVal(DateTime.ParseExact(date_str, formats, CultureInfo.CurrentCulture, DateTimeStyles.None));
					} catch {
						throw new Exception("Unable to parse date string '" + date_str + "'");
					}
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

			private readonly static TType TIME_TYPE = new TDateType(SQLTypes.TIME);

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

			private readonly static TType TIMESTAMP_TYPE = new TDateType(SQLTypes.TIMESTAMP);

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
				return TObject.GetString(d.ToString(format_string));
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TType.StringType;
			}
		}


		#endregion
	}
}