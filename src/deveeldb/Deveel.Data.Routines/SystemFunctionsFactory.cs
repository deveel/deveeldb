using System;
using System.Collections;
using System.Globalization;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Data.Text;
using Deveel.Data.Types;

namespace Deveel.Data.Routines {
	class SystemFunctionsFactory : FunctionFactory {
		private void AddAggregateFunctions() {
			// Aggregate OR
			AddFunction(FunctionBuilder.New("aggor")
				.WithDynamicUnboundedParameter()
				.Aggregate()
				.OnAggregate((resolver, variableResolver, obj1, obj2) => SystemFunctions.Or(obj1, obj2)));

			// AVG
			AddFunction(FunctionBuilder.New("avg")
				.WithDynamicUnboundedParameter()
				.Aggregate()
				.OnAggregate((resolver, variableResolver, ob1, ob2) =>
					ob1 != null ? (ob2.IsNull ? ob1 : (!ob1.IsNull ? ob1.Add(ob2) : ob2)) : ob2)
				.OnAfterAggregate(
					(group, resolver, result) => result.IsNull ? result : result.Divide(TObject.CreateInt4(@group.Count))));

			// COUNT
			AddFunction(FunctionBuilder.New("count")
				.WithDynamicParameter()
				.Aggregate()
				.OnExecute(Count));

			// MAX
			AddFunction(FunctionBuilder.New("max")
				.WithDynamicUnboundedParameter()
				.Aggregate()
				.OnAggregate((resolver, variableResolver, ob1, ob2) => SystemFunctions.Max(ob1, ob2))
				.OnReturnType(FirstArgumentType));

			// MIN
			AddFunction(FunctionBuilder.New("min")
				.Aggregate()
				.WithDynamicUnboundedParameter()
				.OnAggregate((resolver, variableResolver, ob1, ob2) => SystemFunctions.Min(ob1, ob2))
				.OnReturnType(FirstArgumentType));

			// SUM
			AddFunction(FunctionBuilder.New("sum")
				.Aggregate()
				.WithDynamicUnboundedParameter()
				.OnAggregate((resolver, variableResolver, ob1, ob2) => SystemFunctions.Sum(ob1, ob2))
				.OnReturnType(FirstArgumentType));

			AddFunction(FunctionBuilder.New("distinct_count")
				.Aggregate()
				.WithDynamicUnboundedParameter()
				.OnExecute(DistinctCount)
				.ReturnsType(PrimitiveTypes.Numeric));
		}

		private void AddSecurityFunctions() {
			AddFunction(FunctionBuilder.New("user")
				.OnExecute(CurrentUser)
				.ReturnsType(PrimitiveTypes.VarString));
		}

		private void AddSequenceFunctions() {
			// UNIQUEKEY(STRING)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.UniqueKey)
				.WithParameter("table", PrimitiveTypes.VarString)
				.OnExecute(UniqueKey)
				.ReturnsType(PrimitiveTypes.Numeric));

			// CURRVAL(STRING)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.CurrVal)
				.WithParameter("table", PrimitiveTypes.VarString)
				.OnExecute(CurrVal)
				.ReturnsType(PrimitiveTypes.Numeric));

			// SETVAL(STRING, NUMERIC)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.SetVal)
				.WithParameter("table", PrimitiveTypes.VarString)
				.WithParameter("val", PrimitiveTypes.Numeric)
				.OnExecute(SetVal)
				.ReturnsType(PrimitiveTypes.Numeric));

			// NEXTVAL(STRING)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.NextVal)
				.WithParameter("table", PrimitiveTypes.VarString)
				.OnExecute(NextVal)
				.ReturnsType(PrimitiveTypes.Numeric));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.Identity)
				.WithParameter("s", PrimitiveTypes.VarString)
				.OnExecute(Identity)
				.ReturnsType(PrimitiveTypes.Numeric));
		}

		private ExecuteResult Identity(ExecuteContext context) {
			return context.FunctionResult(SystemFunctions.Identity(context.QueryContext, context.EvaluatedArguments[0]));
		}

		private static ExecuteResult CurrVal(ExecuteContext context) {
			return context.FunctionResult(SystemFunctions.CurrVal(context.QueryContext, context.EvaluatedArguments[0]));
		}

		private static ExecuteResult SetVal(ExecuteContext context) {
			return context.FunctionResult(SystemFunctions.SetVal(context.QueryContext, context.EvaluatedArguments[0],
					context.EvaluatedArguments[1]));
		}

		private static ExecuteResult NextVal(ExecuteContext context) {
			return context.FunctionResult(SystemFunctions.NextVal(context.QueryContext, context.EvaluatedArguments[0]));
		}

		private void AddMiscFunctions() {
			// IIF(BOOL, DYNAMIC, DYNAMIC)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Iif)
				.WithParameter("condition", PrimitiveTypes.Boolean)
				.WithDynamicParameter("ifTrue")
				.WithDynamicParameter("ifFalse")
				.OnExecute(Iif)
				.OnReturnType(IifReturnType));

			// CAST(DYNAMIC, STRING)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Cast)
				.WithDynamicParameter("value")
				.WithParameter("destType", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.Cast(args[0], args[1]))
				.OnReturnType(CastReturnType));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.ToNumber)
				.WithParameter("typeString", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.ToNumber(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// VERSION()
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Version)
				.OnExecute(args => SystemFunctions.Version())
				.ReturnsType(PrimitiveTypes.VarString));

			// EXISTS(QUERY)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Exists)
				.WithParameter("query", PrimitiveTypes.QueryPlan)
				.OnExecute(Exists)
				.ReturnsType(PrimitiveTypes.Boolean));

			// UNIQUE(QUERY)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Unique)
				.WithParameter("query", PrimitiveTypes.QueryPlan)
				.OnExecute(Unique)
				.ReturnsType(PrimitiveTypes.Boolean));

			// COALSESCE([DYNAMIC])
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Coalesce)
				.WithDynamicUnboundedParameter()
				.OnExecute(SystemFunctions.Coalesce)
				.OnReturnType(CoalesceReturnType));

			// GREATEST([DYNAMIC])
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Greatest)
				.WithDynamicUnboundedParameter()
				.OnExecute(SystemFunctions.Greatest)
				.OnReturnType(FirstArgumentType));

			// LEAST([DYNAMIC])
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Least)
				.WithDynamicUnboundedParameter()
				.OnExecute(SystemFunctions.Least)
				.OnReturnType(FirstArgumentType));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.NullIf)
				.WithDynamicParameter("a")
				.WithDynamicParameter("b")
				.OnExecute(args => SystemFunctions.NullIf(args[0], args[1]))
				.OnReturnType(FirstArgumentType));
		}

		private void AddInternalFunctions() {
			AddFunction(FunctionBuilder.New("i_frule_convert")
				.WithDynamicParameter("foreignKey")
				.OnExecute(args => SystemFunctions.FRuleConvert(args[0]))
				.OnReturnType(FRuleConvertReturnType));

			AddFunction(FunctionBuilder.New("i_view_data")
				.WithParameter("command", PrimitiveTypes.VarString)
				.WithParameter("binary", PrimitiveTypes.BinaryType)
				.OnExecute(args => SystemFunctions.ViewData(args[0], args[1]))
				.ReturnsType(PrimitiveTypes.VarString));

			AddFunction(FunctionBuilder.New("i_privilege_string")
				.WithParameter("privBit", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.PrivilegeString(args[0]))
				.ReturnsType(PrimitiveTypes.VarString));

			AddFunction(FunctionBuilder.New("i_sql_type")
				.WithParameter("typeString", PrimitiveTypes.VarString)
				.WithParameter("size", PrimitiveTypes.Numeric)
				.WithParameter("scale", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.SqlTypeString(args[0], args[1], args[2]))
				.ReturnsType(PrimitiveTypes.VarString));

			AddFunction(FunctionBuilder.New("_new_Object")
				.WithParameter("typeString", PrimitiveTypes.VarString)
				.WithDynamicUnboundedParameter()
				.OnExecute(NewObject)
				.OnReturnType(NewObjectReturnType));
		}

		private static ExecuteResult NewObject(ExecuteContext context) {
			var type = context.EvaluatedArguments[0];
			var args = new TObject[context.ArgumentCount - 1];
			Array.Copy(context.EvaluatedArguments, 1, args, 0, context.ArgumentCount-1);
			return context.FunctionResult(SystemFunctions.Instantiate(context.QueryContext, type, args));
		}

		private static TType NewObjectReturnType(ExecuteContext context) {
			return TType.GetObjectType(context.EvaluatedArguments[0].ToStringValue());
		}

		private void AddStringFunctions() {
			// CONCAT([STRING])
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Concat)
				.WithUnboundedParameter("strings", PrimitiveTypes.VarString)
				.OnExecute(SystemFunctions.Concat)
				.OnReturnType(ConcatReturnTType));

			// REPLACE(STRING, STRING, STRING)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Replace)
				.WithParameter("s", PrimitiveTypes.VarString)
				.WithParameter("oldValue", PrimitiveTypes.VarString)
				.WithParameter("newValue", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.Replace(args[0], args[1], args[2]))
				.OnReturnType(FirstArgumentType));

			// SUBSTRING(STRING, NUMERIC)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Substring)
				.WithParameter("s", PrimitiveTypes.VarString)
				.WithParameter("start", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Substring(args[0], args[1]))
				.ReturnsType(PrimitiveTypes.VarString));

			// SUBSTRING(STRING, NUMERIC, NUMERIC)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Substring)
				.WithParameter("s", PrimitiveTypes.VarString)
				.WithParameter("start", PrimitiveTypes.Numeric)
				.WithParameter("end", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Substring(args[0], args[1], args[2]))
				.ReturnsType(PrimitiveTypes.VarString));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.InStr)
				.WithParameter("s", PrimitiveTypes.VarString)
				.WithParameter("search", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.InString(args[0], args[1]))
				.ReturnsType(PrimitiveTypes.Numeric));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.InStr)
				.WithParameter("s", PrimitiveTypes.VarString)
				.WithParameter("search", PrimitiveTypes.VarString)
				.WithParameter("start", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.InString(args[0], args[1], args[2]))
				.ReturnsType(PrimitiveTypes.Numeric));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.InStr)
				.WithParameter("s", PrimitiveTypes.VarString)
				.WithParameter("search", PrimitiveTypes.VarString)
				.WithParameter("start", PrimitiveTypes.Numeric)
				.WithParameter("end", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.InString(args[0], args[1], args[2], args[3]))
				.ReturnsType(PrimitiveTypes.Numeric));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.LeftPad)
				.WithParameter("s", PrimitiveTypes.VarString)
				.WithParameter("totalWidth", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.LPad(args[0], args[1]))
				.OnReturnType(FirstArgumentType));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.LeftPad)
				.WithParameter("s", PrimitiveTypes.VarString)
				.WithParameter("totalWidth", PrimitiveTypes.Numeric)
				.WithParameter("pad", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.LPad(args[0], args[1], args[2]))
				.OnReturnType(FirstArgumentType));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.RightPad)
				.WithParameter("s", PrimitiveTypes.VarString)
				.WithParameter("totalWidth", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.RPad(args[0], args[1]))
				.OnReturnType(FirstArgumentType));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.RightPad)
				.WithParameter("s", PrimitiveTypes.VarString)
				.WithParameter("totalWidth", PrimitiveTypes.Numeric)
				.WithParameter("pad", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.RPad(args[0], args[1], args[2]))
				.OnReturnType(FirstArgumentType));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.CharLength)
				.WithParameter("s", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.CharLength(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.Soundex)
				.WithParameter("s", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.Soundex(args[0]))
				.ReturnsType(PrimitiveTypes.VarString));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.Upper)
				.WithParameter("s", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.Upper(args[0]))
				.OnReturnType(FirstArgumentType));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.Lower)
				.WithParameter("s", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.Lower(args[0]))
				.OnReturnType(FirstArgumentType));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.Trim)
				.WithParameter("s", PrimitiveTypes.VarString)
				.WithParameter("trimType", PrimitiveTypes.VarString)
				.WithParameter("toTrim", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.Trim(args[2], args[0], args[1]))
				.OnReturnType(context => ReturnTType(context.Arguments[2], context)));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.LeftTrim)
				.WithParameter("s", PrimitiveTypes.VarString)
				.WithParameter("toTrim", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.LTrim(args[0], args[1]))
				.OnReturnType(FirstArgumentType));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.LeftTrim)
				.WithParameter("s", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.LTrim(args[0]))
				.OnReturnType(FirstArgumentType));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.RightTrim)
				.WithParameter("s", PrimitiveTypes.VarString)
				.WithParameter("toTrim", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.RTrim(args[0], args[1]))
				.OnReturnType(FirstArgumentType));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.RightTrim)
				.WithParameter("s", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.RTrim(args[0]))
				.OnReturnType(FirstArgumentType));
		}

		private void AddArithmeticFunctions() {
			// PI()
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Pi)
				.OnExecute(args => SystemFunctions.Pi())
				.ReturnsType(PrimitiveTypes.Numeric));

			// E()
			AddFunction(FunctionBuilder.New(SystemFunctionNames.E)
				.OnExecute(args => SystemFunctions.E())
				.ReturnsType(PrimitiveTypes.Numeric));

			// SQRT(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Sqrt)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Sqrt(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// LOG(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Log)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Log(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// LOG(n, newBase)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Log)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.WithParameter("newBase", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Log(args[0], args[1]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// LOG10(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Log10)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Log10(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// TAN(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Tan)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Tan(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// TANH(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.TanH)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.TanH(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.ATan)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.ATan(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// ROUND(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Round)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Round(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// ROUND(n, type)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Round)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.WithParameter("type", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.Round(args[0], args[1]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// POW(n, exp)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Pow)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.WithParameter("exp", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Pow(args[0], args[1]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// ABS(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Abs)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Abs(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// COS(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Cos)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Cos(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// COSH(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.CosH)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.CosH(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// ACOS(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.ACos)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.ACos(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// COT(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Cot)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Cot(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// MOD(a, b)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Mod)
				.WithParameter("a", PrimitiveTypes.Numeric)
				.WithParameter("b", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Mod(args[0], args[1]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// SIN(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Sin)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Sin(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// ASIN(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.ASin)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.ASin(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// SINH(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.SinH)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.SinH(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// ARC(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Arc)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Arc(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// EXP(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Exp)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Exp(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// RADIANS(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Radians)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Radians(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// DEGREES(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Degrees)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Degrees(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// RAND()
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Rand)
				.OnExecute(Rand)
				.ReturnsType(PrimitiveTypes.Numeric));

			// CEIL(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Ceil)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Ceil(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// FLOOR(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Floor)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Floor(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// SIGN(n)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Sign)
				.WithParameter("n", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.Signum(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));
		}

		private void AddDateFunctions() {
			// DATE(s), TODATE(s)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Date)
				.WithParameter("s", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.ToDate(args[0]))
				.ReturnsType(TType.GetDateType(SqlType.Date)));

			// DATE(), CURRENT_DATE
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Date)
				.OnExecute(args => SystemFunctions.CurrentDate())
				.ReturnsType(TType.GetDateType(SqlType.Date)));

			// TIME(s), TOTIME(s)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Time)
				.WithParameter("s", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.ToTime(args[0]))
				.ReturnsType(TType.GetDateType(SqlType.Time)));

			// TIME(), CURRENT_TIME
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Time)
				.OnExecute(args => SystemFunctions.CurrentTime())
				.ReturnsType(TType.GetDateType(SqlType.Time)));

			// TIMESTAMPOB(s)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.TimeStamp)
				.WithParameter("s", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.ToTimeStamp(args[0]))
				.ReturnsType(TType.GetDateType(SqlType.TimeStamp)));

			// TIMESTAMP(), CURRENT_TIMESTAMP
			AddFunction(FunctionBuilder.New(SystemFunctionNames.TimeStamp)
				.OnExecute(args => SystemFunctions.CurrentTimestamp())
				.ReturnsType(TType.GetDateType(SqlType.TimeStamp)));

			// DBTIMEZONE()
			AddFunction(FunctionBuilder.New(SystemFunctionNames.DbTimeZone)
				.OnExecute(args => SystemFunctions.DbTimeZone())
				.ReturnsType(PrimitiveTypes.VarString));

			// EXTRACT(STRING, DATE)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Extract)
				.WithParameter("field", PrimitiveTypes.VarString)
				.WithParameter("date", PrimitiveTypes.Date)
				.OnExecute(args => SystemFunctions.Extract(args[0], args[1]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// YEAR(DATE), YEAR(INTERVAL)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.Year)
				.WithParameter("date", PrimitiveTypes.Date)
				.OnExecute(args => SystemFunctions.Year(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.Month)
				.WithParameter("date", PrimitiveTypes.Date)
				.OnExecute(args => SystemFunctions.Month(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.Day)
				.WithParameter("date", PrimitiveTypes.Date)
				.OnExecute(args => SystemFunctions.Day(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.Hour)
				.WithParameter("date", PrimitiveTypes.Date)
				.OnExecute(args => SystemFunctions.Hour(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.Minute)
				.WithParameter("date", PrimitiveTypes.Date)
				.OnExecute(args => SystemFunctions.Minute(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.Second)
				.WithParameter("date", PrimitiveTypes.Date)
				.OnExecute(args => SystemFunctions.Second(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.Millis)
				.WithParameter("date", PrimitiveTypes.Date)
				.OnExecute(args => SystemFunctions.Millis(args[0]))
				.ReturnsType(PrimitiveTypes.Numeric));

			// DATEFORMAT(DATE, STRING)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.DateFormat)
				.WithParameter("date", PrimitiveTypes.Date)
				.WithParameter("format", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.DateFormat(args[0], args[1]))
				.ReturnsType(PrimitiveTypes.VarString));

			// NEXT_DAY(DATE, STRING)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.NextDay)
				.WithParameter("date", PrimitiveTypes.Date)
				.WithParameter("weekday", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.NextDay(args[0], args[1]))
				.ReturnsType(PrimitiveTypes.Date));

			// ADD_MONTHS(DATE, NUMERIC)
			AddFunction(FunctionBuilder.New(SystemFunctionNames.AddMonths)
				.WithParameter("date", PrimitiveTypes.Date)
				.WithParameter("months", PrimitiveTypes.Numeric)
				.OnExecute(args => SystemFunctions.AddMonths(args[0], args[1]))
				.ReturnsType(PrimitiveTypes.Date));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.MonthsBetween)
				.WithParameter("firstDate", PrimitiveTypes.Date)
				.WithParameter("secondDate", PrimitiveTypes.Date)
				.OnExecute(args => SystemFunctions.MonthsBetween(args[0], args[1]))
				.ReturnsType(PrimitiveTypes.Numeric));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.LastDay)
				.WithParameter("date", PrimitiveTypes.Date)
				.OnExecute(args => SystemFunctions.LastDay(args[0]))
				.ReturnsType(PrimitiveTypes.Date));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.Interval)
				.WithParameter("s", PrimitiveTypes.VarString)
				.WithParameter("field", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.ToInterval(args[0], args[1]))
				.ReturnsType(PrimitiveTypes.IntervalType));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.Interval)
				.WithParameter("s", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.ToInterval(args[0]))
				.ReturnsType(PrimitiveTypes.IntervalType));
		}

		private void AddBinaryFunctions() {
			AddFunction(FunctionBuilder.New(SystemFunctionNames.HexToBinary)
				.WithParameter("hex", PrimitiveTypes.VarString)
				.OnExecute(args => SystemFunctions.HexToBinary(args[0]))
				.ReturnsType(PrimitiveTypes.BinaryType));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.BinaryToHex)
				.WithParameter("bin", PrimitiveTypes.BinaryType)
				.OnExecute(args => SystemFunctions.BinaryToHex(args[0]))
				.ReturnsType(PrimitiveTypes.VarString));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.Crc32)
				.WithParameter("bin", PrimitiveTypes.BinaryType)
				.OnExecute(args => SystemFunctions.Crc32(args[0]))
				.ReturnsType(PrimitiveTypes.BinaryType));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.Adler32)
				.WithParameter("bin", PrimitiveTypes.BinaryType)
				.OnExecute(args => SystemFunctions.Adler32(args[0]))
				.ReturnsType(PrimitiveTypes.BinaryType));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.Compress)
				.WithParameter("bin", PrimitiveTypes.BinaryType)
				.OnExecute(args => SystemFunctions.Compress(args[0]))
				.ReturnsType(PrimitiveTypes.BinaryType));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.Uncompress)
				.WithParameter("bin", PrimitiveTypes.BinaryType)
				.OnExecute(args => SystemFunctions.Uncompress(args[0]))
				.ReturnsType(PrimitiveTypes.BinaryType));

			AddFunction(FunctionBuilder.New(SystemFunctionNames.OctetLength)
				.WithParameter("bin", PrimitiveTypes.BinaryType)
				.OnExecute(args => SystemFunctions.OctetLength(args[0]))
				.ReturnsType(PrimitiveTypes.BinaryType));
		}

		protected override void OnInit() {
			AddAggregateFunctions();
			AddSecurityFunctions();
			AddSequenceFunctions();
			AddMiscFunctions();

			AddInternalFunctions();
			AddStringFunctions();
			AddArithmeticFunctions();
			AddDateFunctions();
			AddBinaryFunctions();
		}

		private static TType FirstArgumentType(ExecuteContext context) {
			return ReturnTType(context.Arguments[0], context);
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
			var argType = ReturnTType(context.Arguments[0], context);
			return argType is TStringType ? (TType)PrimitiveTypes.Numeric : PrimitiveTypes.VarString;
		}

		private static ExecuteResult Exists(ExecuteContext context) {
			return context.FunctionResult(SystemFunctions.Exists(context.EvaluatedArguments[0], context.QueryContext));
		}

		private static ExecuteResult Unique(ExecuteContext context) {
			return context.FunctionResult(SystemFunctions.IsUnique(context.EvaluatedArguments[0], context.QueryContext));
		}

		private static ExecuteResult CurrentUser(ExecuteContext context) {
			return context.FunctionResult(TObject.CreateString(context.QueryContext.UserName));
		}

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
			TType t1 = ReturnTType(context.Arguments[1], context);
			// This is a hack for null values.  If the first parameter is null
			// then return the type of the second parameter which hopefully isn't
			// also null.
			if (t1 is TNullType) {
				return ReturnTType(context.Arguments[2], context);
			}
			return t1;
		}

		private static ExecuteResult UniqueKey(ExecuteContext context) {
			return context.FunctionResult(SystemFunctions.UniqueKey(context.QueryContext, context.EvaluatedArguments[0]));
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
				for (int m = 0; m < cols && firstIsNull; ++m) {
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

		private static ExecuteResult Rand(ExecuteContext context) {
			return new ExecuteResult(context) {ReturnValue = SystemFunctions.Rand(context.QueryContext)};
		}

		private static TType ConcatReturnTType(ExecuteContext context) {
			// Determine the locale of the first string parameter.
			CultureInfo locale = null;
			CollationStrength strength = 0;
			CollationDecomposition decomposition = 0;
			for (int i = 0; i < context.ArgumentCount && locale == null; ++i) {
				TType type = ReturnTType(context.Arguments[i], context);
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

		private static TObject Evaluate(Expression exp, ExecuteContext context) {
			return exp.Evaluate(context.GroupResolver, context.VariableResolver, context.QueryContext);
		}

		private static TType ReturnTType(Expression exp, ExecuteContext context) {
			return exp.ReturnTType(context.VariableResolver, context.QueryContext);
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
	}
}