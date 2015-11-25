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
using Deveel.Data.Types;

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

		private ExecuteResult Simple(ExecuteContext context, Func<DataObject[], DataObject> func) {
			var evaluated = context.EvaluatedArguments;
			var value = func(evaluated);
			return context.Result(value);
		}

		private ExecuteResult Binary(ExecuteContext context, Func<DataObject, DataObject, DataObject> func) {
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

			// COUNT
			Register(config => config.Named("count")
				.WithUnoundedParameter("args", Function.DynamicType)
				.WhenExecute(Count.Execute)
				.OfAggregateType()
				.ReturnsNumeric());

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
		}

		private void AddMiscFunctions() {
			Register(config => config.Named("iif")
				.WithBooleanParameter("condition")
				.WithDynamicParameter("ifTrue")
				.WithDynamicParameter("ifFalse")
				.WhenExecute(context => SystemFunctions.Iif(context))
				.ReturnsType(Function.DynamicType));

			Register(config => config.Named("i_frule_convert")
			.WithDynamicParameter("rule")
			.WhenExecute(context => Simple(context, args => SystemFunctions.FRuleConvert(args[0])))
			.ReturnsType(context => {
				var argType = ReturnType(context.Arguments[0], context);
				return argType is StringType ? (SqlType)PrimitiveTypes.Numeric() : (SqlType)PrimitiveTypes.String();
			}));
		}

		private static SqlType ReturnType(SqlExpression exp, ExecuteContext context) {
			return exp.ReturnType(context.Request, context.VariableResolver);
		}

		protected override void OnInit() {
			AddAggregateFunctions();

			AddConversionFunctions();
			AddSecurityFunctions();
			AddSequenceFunctions();

			AddMiscFunctions();
		}

		#region Count

		private static class Count {
			public static ExecuteResult Execute(ExecuteContext context) {
				if (context.GroupResolver == null)
					throw new Exception("'count' can only be used as an aggregate function.");

				int size = context.GroupResolver.Count;
				DataObject result;
				// if, count(*)
				if (size == 0 || context.Invoke.IsGlobArgument) {
					result = DataObject.Integer(size);
				} else {
					// Otherwise we need to count the number of non-null entries in the
					// columns list(s).

					int totalCount = size;

					var exp = context.Arguments[0];
					for (int i = 0; i < size; ++i) {
						var val = exp.EvaluateToConstant(context.Request, context.GroupResolver.GetVariableResolver(i));
						if (val.IsNull) {
							--totalCount;
						}
					}

					result = DataObject.Integer(totalCount);
				}

				return context.Result(result);
			}
		}

		#endregion

		#region DistinctCount

		static class DistinctCount {
			public static ExecuteResult Execute(ExecuteContext context) {
				throw new NotImplementedException();
			}
		}

		#endregion

		#region Cast

		static class Cast {
			public static ExecuteResult Execute(ExecuteContext context) {
				var value = context.EvaluatedArguments[0];
				var typeArg = context.EvaluatedArguments[1];
				var typeString = typeArg.AsVarChar().Value.ToString();
				var type = SqlType.Parse(context.Request.Context, typeString);

				return context.Result(SystemFunctions.Cast(value, type));
			}

			public static SqlType ReturnType(ExecuteContext context) {
				var typeArg = context.EvaluatedArguments[1];
				var typeString = typeArg.AsVarChar().Value.ToString();
				return SqlType.Parse(context.Request.Context, typeString);
			}
		}

		#endregion
	}
}
