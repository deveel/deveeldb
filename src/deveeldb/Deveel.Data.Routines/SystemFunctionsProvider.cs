// 
//  Copyright 2010-2015 Deveel
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
//


using System;
using System.Collections.Generic;

using Deveel.Data;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Fluid;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
	class SystemFunctionsProvider : FunctionProvider {
		public override string SchemaName {
			get { return SystemSchema.Name; }
		}

		protected override ObjectName NormalizeName(ObjectName functionName) {
			if (functionName.Parent == null)
				return new ObjectName(new ObjectName(SchemaName), functionName.Name);

			return base.NormalizeName(functionName);
		}

		private InvokeResult Simple(InvokeContext context, Func<Field[], Field> func) {
			var evaluated = context.EvaluatedArguments;
			var value = func(evaluated);
			return context.Result(value);
		}

		private InvokeResult Simple(InvokeContext context, Func<Field> func) {
			var value = func();
			return context.Result(value);
		}

		private InvokeResult Binary(InvokeContext context, Func<Field, Field, Field> func) {
			var evaluated = context.EvaluatedArguments;
			var value = func(evaluated[0], evaluated[1]);
			return context.Result(value);			
		}

		private void AddAggregateFunctions() {
			Register(configuration => configuration
				.Named("aggor")
				.WithParameter(p => p.Named("args").Unbounded().OfDynamicType())
				.OfAggregateType()
				.WhenExecute(context => Binary(context, SystemFunctions.Or)));

			Register(config => config.Named("count")
				.WithUnoundedParameter("args", Function.DynamicType)
				.WhenExecute(Count.Execute)
				.OfAggregateType()
				.ReturnsNumeric());

			Register(config => config
				.Named("avg")
				.WithUnoundedParameter("args", Function.DynamicType)
				.WhenExecute(context => Simple(context, args => {
					var ob1 = args[0];
					var ob2 = args[1];
					if (Field.IsNullField(ob1))
						return ob2;
					if (Field.IsNullField(ob2))
						return ob1;

					return ob1.Add(ob2);
				}))
				.OfAggregateType()
				.OnAfterAggregate(
					(context, result) => result.IsNull ? result : result.Divide(Field.Integer(context.GroupResolver.Count))));
		}

		private void AddSecurityFunctions() {
			Register(config => config.Named("user")
				.WhenExecute(context => context.Result(SystemFunctions.User(context.Request)))
				.ReturnsString());
		}

		private void AddConversionFunctions() {
			Register(config => config.Named("cast")
				.WithDynamicParameter("value")
				.WithStringParameter("destType")
				.WhenExecute(Cast.Execute)
				.ReturnsType(Cast.ReturnType));

			Register(config => config.Named("tonumber")
				.WithDynamicParameter("value")
				.WhenExecute(context => Simple(context, args => SystemFunctions.ToNumber(args[0])))
				.ReturnsNumeric());

			Register(config => config.Named("tostring")
				.WithDynamicParameter("value")
				.WhenExecute(context => Simple(context, args => SystemFunctions.ToString(args[0])))
				.ReturnsString());

			Register(config => config.Named("tobinary")
				.WithDynamicParameter("value")
				.WhenExecute(context => Simple(context, args => SystemFunctions.ToBinary(args[0])))
				.ReturnsType(PrimitiveTypes.Binary()));

			// Date Conversions
			Register(config => config.Named("todate")
				.WithStringParameter("value")
				.WhenExecute(context => Simple(context, objects => SystemFunctions.ToDate(objects[0])))
				.ReturnsType(PrimitiveTypes.Date()));

			Register(config => config.Named("todatetime")
				.WithStringParameter("value")
				.WhenExecute(context => Simple(context, args => SystemFunctions.ToDateTime(args[0])))
				.ReturnsType(PrimitiveTypes.DateTime()));

			Register(config => config.Named("totimestamp")
				.WithParameter(p => p.Named("value").OfStringType())
				.WhenExecute(context => Simple(context, args => SystemFunctions.ToTimeStamp(args[0])))
				.ReturnsType(PrimitiveTypes.TimeStamp()));
		}

		private void AddSequenceFunctions() {
			Register(config => config.Named("uniquekey")
				.WithStringParameter("table")
				.WhenExecute(context => Simple(context, args => SystemFunctions.UniqueKey(context.Request, args[0])))
				.ReturnsNumeric());

			Register(config => config.Named("curval")
				.WithStringParameter("table")
				.WhenExecute(context => Simple(context, args => SystemFunctions.CurrentValue(context.Request, args[0])))
				.ReturnsNumeric());

			Register(config => config.Named("nextval")
				.WithParameter("sequence", PrimitiveTypes.String())
				.WhenExecute(context => Simple(context, args => SystemFunctions.NextValue(context.Request, args[0])))
				.ReturnsNumeric());
		}

		private static SqlType IifReturnType(InvokeContext context) {
			// It's impossible to know the return type of this function until runtime
			// because either comparator could be returned.  We could assume that
			// both branch expressions result in the same type of object but this
			// currently is not enforced.

			// Returns type of first argument
			var t1 = ReturnType(context.Arguments[1], context);
			// This is a hack for null values.  If the first parameter is null
			// then return the type of the second parameter which hopefully isn't
			// also null.
			if (t1 is NullType) {
				return ReturnType(context.Arguments[2], context);
			}
			return t1;
		}

		private void AddMiscFunctions() {
			Register(config => config.Named("iif")
				.WithBooleanParameter("condition")
				.WithDynamicParameter("ifTrue")
				.WithDynamicParameter("ifFalse")
				.WhenExecute(context => SystemFunctions.Iif(context))
				.ReturnsType(IifReturnType));

			Register(config => config.Named("i_frule_convert")
			.WithDynamicParameter("rule")
			.WhenExecute(context => Simple(context, args => SystemFunctions.FRuleConvert(args[0])))
			.ReturnsType(context => {
				var argType = ReturnType(context.Arguments[0], context);
				return argType is StringType ? (SqlType)PrimitiveTypes.Numeric() : (SqlType)PrimitiveTypes.String();
			}));
		}

		private void AddDateFunctions() {
			Register(config => config.Named("date")
				.WhenExecute(context => Simple(context, () => SystemFunctions.CurrentDate(context.Request)))
				.ReturnsType(PrimitiveTypes.Date()));

			Register(config => config.Named("time")
				.WhenExecute(context => Simple(context, () => SystemFunctions.CurrentTime(context.Request)))
				.ReturnsType(PrimitiveTypes.Time()));

			Register(config => config.Named("timestamp")
				.WhenExecute(context => Simple(context, () => SystemFunctions.CurrentTimeStamp(context.Request)))
				.ReturnsDateTime());

			Register(config => config.Named("system_date")
				.WhenExecute(context => Simple(context, SystemFunctions.SystemDate))
				.ReturnsType(PrimitiveTypes.Date()));

			Register(config => config.Named("system_time")
				.WhenExecute(context => Simple(context, SystemFunctions.SystemTime))
				.ReturnsType(PrimitiveTypes.Time()));

			Register(config => config.Named("system_timestamp")
				.WhenExecute(context => Simple(context, SystemFunctions.SystemTimeStamp))
				.ReturnsDateTime());

			Register(config => config.Named("add_date")
			.WithDateTimeParameter("date")
			.WithStringParameter("datePart")
			.WithNumericParameter("value")
			.WhenExecute(context => Simple(context, args => SystemFunctions.AddDate(args[0], args[1], args[2])))
			.ReturnsDateTime());
		}

		private static SqlType ReturnType(SqlExpression exp, InvokeContext context) {
			return exp.ReturnType(context.Request, context.VariableResolver);
		}

		protected override void OnInit() {
			AddAggregateFunctions();

			AddConversionFunctions();
			AddSecurityFunctions();
			AddSequenceFunctions();
			AddDateFunctions();

			AddMiscFunctions();
		}

		#region Count

		private static class Count {
			public static InvokeResult Execute(InvokeContext context) {
				if (context.GroupResolver == null)
					throw new Exception("'count' can only be used as an aggregate function.");

				int size = context.GroupResolver.Count;
				Field result;
				// if, count(*)
				if (size == 0 || context.Invoke.IsGlobArgument) {
					result = Field.Integer(size);
				} else {
					// Otherwise we need to count the number of non-null entries in the
					// columns list(s)

					var arg1 = context.EvaluatedArguments[0];
					var arg2 = context.EvaluatedArguments[1];

					if (!Field.IsNullField(arg2)) {
						if (Field.IsNullField(arg1)) {
							result = Field.Integer(1);
						} else {
							result = arg1.Add(Field.Integer(1));
						}
					} else {
						result = arg1;
					}
				}

				return context.Result(result);
			}
		}

		#endregion

		#region DistinctCount

		static class DistinctCount {
			public static InvokeResult Execute(InvokeContext context) {
				throw new NotImplementedException();
			}
		}

		#endregion

		#region Cast

		static class Cast {
			public static InvokeResult Execute(InvokeContext context) {
				var value = context.EvaluatedArguments[0];
				var typeArg = context.EvaluatedArguments[1];
				var typeString = typeArg.AsVarChar().Value.ToString();
				var type = SqlType.Parse(context.Request.Context, typeString);

				return context.Result(SystemFunctions.Cast(value, type));
			}

			public static SqlType ReturnType(InvokeContext context) {
				var typeArg = context.EvaluatedArguments[1];
				var typeString = typeArg.AsVarChar().Value.ToString();
				return SqlType.Parse(context.Request.Context, typeString);
			}
		}

		#endregion
	}
}
