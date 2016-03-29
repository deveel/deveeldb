// 
//  Copyright 2010-2016 Deveel
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
using Deveel.Math;

namespace Deveel.Data.Routines {
	class SystemFunctionsProvider : FunctionProvider {
		#region Utils

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

		private static SqlType ReturnType(SqlExpression exp, InvokeContext context) {
			return exp.ReturnType(context.Request, context.VariableResolver);
		}

		#endregion

		#region Aggregate Functions

		private void AggregateFunctions() {
			// AGGOR
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

			// COUNT(DISTINCT)
			Register(new DistinctCountFucntion());


			//  SUM
			Register(config => config
				.Named("sum")
				.WithUnoundedParameter("args", Function.DynamicType)
				.OfAggregateType()
				.WhenExecute(context => Simple(context, args => {
					var ob1 = args[0];
					var ob2 = args[1];
					if (Field.IsNullField(ob1))
						return ob2;
					if (Field.IsNullField(ob2))
						return ob1;

					return ob1.Add(ob2);
				}))
				.ReturnsNumeric());

			// MAX
			Register(config => config
				.Named("max")
				.WithUnoundedParameter("args", Function.DynamicType)
				.OfAggregateType()
				.WhenExecute(context => Simple(context, args => {
					var ob1 = args[0];
					var ob2 = args[1];
					if (Field.IsNullField(ob1))
						return ob2;
					if (Field.IsNullField(ob2))
						return ob1;

					if (ob1.IsGreaterThan(ob2))
						return ob1;
					if (ob2.IsGreaterThan(ob1))
						return ob2;

					return ob1;
				}))
				.ReturnsNumeric());

			// MIN
			Register(config => config
				.Named("min")
				.WithUnoundedParameter("args", Function.DynamicType)
				.OfAggregateType()
				.WhenExecute(context => Simple(context, args => {
					var ob1 = args[0];
					var ob2 = args[1];
					if (Field.IsNullField(ob1))
						return ob2;
					if (Field.IsNullField(ob2))
						return ob1;

					if (ob1.IsSmallerThan(ob2))
						return ob1;
					if (ob2.IsSmallerThan(ob1))
						return ob2;

					return ob1;
				}))
				.ReturnsNumeric());

			// AVG
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

		class DistinctCountFucntion : SystemFunction {
			public DistinctCountFucntion()
				: base("distinct_count",
					new[] { new RoutineParameter("args", Function.DynamicType, ParameterAttributes.Unbounded) }, PrimitiveTypes.Integer(),
					FunctionType.Aggregate) {
			}

			public override InvokeResult Execute(InvokeContext context) {
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
					return context.Result(Field.Integer(rows));
				}

				// Make an array of all cells in the group that we are finding which
				// are distinct.
				int cols = context.ArgumentCount;
				var groupRow = new Field[rows * cols];
				int n = 0;
				for (int i = 0; i < rows; ++i) {
					var vr = context.GroupResolver.GetVariableResolver(i);
					for (int p = 0; p < cols; ++p) {
						var exp = context.Arguments[p];
						groupRow[n + p] = exp.EvaluateToConstant(context.Request, vr);
					}

					n += cols;
				}

				var c = new DistinctComparer(cols, groupRow);

				// The list of indexes,
				var list = new int[rows];
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
						throw new Exception("Assertion failed - the distinct list does not " +
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
						var val = groupRow[(firstEntry * cols) + m];
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

				return context.Result(Field.Integer(distinctCount));
			}

			#region DistinctComparer

			class DistinctComparer : IComparer<int> {
				private readonly int columnCount;
				private readonly Field[] groupedRows;

				public DistinctComparer(int columnCount, Field[] groupedRows) {
					this.columnCount = columnCount;
					this.groupedRows = groupedRows;
				}

				public int Compare(int x, int y) {
					int index1 = x * columnCount;
					int index2 = y * columnCount;
					for (int n = 0; n < columnCount; ++n) {
						int v = groupedRows[index1 + n].CompareTo(groupedRows[index2 + n]);
						if (v != 0) {
							return v;
						}
					}

					// If we got here then rows must be equal.
					return 0;
				}
			}

			#endregion
		}

		#endregion

		#endregion

		#region Security Functions

		private void SecurityFunctions() {
			Register(config => config.Named("user")
				.WhenExecute(context => context.Result(SystemFunctions.User(context.Request)))
				.ReturnsString());
		}

		#endregion


		#region Conversion Functions

		private void ConversionFunctions() {
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

		#endregion

		#region Sequence Functions

		private void SequenceFunctions() {
			Register(config => config.Named("uniquekey")
				.WithStringParameter("table")
				.WhenExecute(context => Simple(context, args => SystemFunctions.UniqueKey(context.Request, args[0])))
				.ReturnsNumeric());

			Register(config => config.Named("curval")
				.WithStringParameter("sequence")
				.WhenExecute(context => Simple(context, args => SystemFunctions.CurrentValue(context.Request, args[0])))
				.ReturnsNumeric());

			Register(config => config.Named("nextval")
				.WithParameter("sequence", PrimitiveTypes.String())
				.WhenExecute(context => Simple(context, args => SystemFunctions.NextValue(context.Request, args[0])))
				.ReturnsNumeric());

			Register(config => config
				.Named("curkey")
				.WithStringParameter("table")
				.WhenExecute(context => Simple(context, args => SystemFunctions.CurrentKey(context.Request, args[0])))
				.ReturnsNumeric());
		}

		#endregion

		#region Misc Functions

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

		private void MiscFunctions() {
			Register(config => config.Named("iif")
				.WithBooleanParameter("condition")
				.WithDynamicParameter("ifTrue")
				.WithDynamicParameter("ifFalse")
				.WhenExecute(context => Simple(context, args => {
					Field result = Field.Null();

					var condition = args[0];
					if (condition.Type is BooleanType) {
						if (condition.Equals(Field.BooleanTrue)) {
							result = args[1];
						} else if (condition.Equals(Field.BooleanFalse)) {
							result = args[2];
						}
					}

					return result;
				}))
				.ReturnsType(IifReturnType));

			Register(config => config.Named("i_frule_convert")
				.WithDynamicParameter("rule")
				.WhenExecute(context => Simple(context, args => SystemFunctions.FRuleConvert(args[0])))
				.ReturnsType(context => {
					var argType = ReturnType(context.Arguments[0], context);
					return argType is StringType ? (SqlType) PrimitiveTypes.Numeric() : (SqlType) PrimitiveTypes.String();
				}));

			// VERSION
			Register(config => config
			.Named("version")
			.WhenExecute(context => context.Result(Field.String(context.Request.Query.Session.Database().Version.ToString(3))))
			.ReturnsString());

			// COALESCE
			Register(config => config.Named("coalesce")
			.WhenExecute(context => Simple(context, args => {
				var argc = args.Length;
				for (int i = 0; i < argc; i++) {
					var arg = args[i];
					if (!Field.IsNullField(arg))
						return arg;
				}

				return Field.Null();
			}))
			.ReturnsType(context => {
				var argc = context.Arguments.Length;
				for (int i = 0; i < argc; i++) {
					var returnType = context.Arguments[i].ReturnType(context.Request, context.VariableResolver);
					if (!(returnType is NullType))
						return returnType;
				}

				return PrimitiveTypes.Null();
			}));
		}

		#endregion

		#region Date Functions

		private void DateFunctions() {
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

			Register(config => config
				.Named("system_timestamp")
				.WhenExecute(context => Simple(context, SystemFunctions.SystemTimeStamp))
				.ReturnsDateTime());

			// ADD_DATE
			Register(config => config.Named("add_date")
				.WithDateTimeParameter("date")
				.WithStringParameter("datePart")
				.WithNumericParameter("value")
				.WhenExecute(context => Simple(context, args => SystemFunctions.AddDate(args[0], args[1], args[2])))
				.ReturnsDateTime());

			// EXTRACT
			Register(config => config.Named("extract")
				.WithDateTimeParameter("date")
				.WithStringParameter("unit")
				.WhenExecute(context => Simple(context, args => SystemFunctions.Extract(args[0], args[1])))
				.ReturnsNumeric());

			// DATEFORMAT
			Register(config => config.Named("dateformat")
				.WithDateTimeParameter("date")
				.WithStringParameter("format")
				.WhenExecute(context => Simple(context, args => SystemFunctions.DateFormat(args[0], args[1])))
				.ReturnsString());

			// NEXT_DAY
			Register(config => config
				.Named("next_day")
				.WithDateTimeParameter("date")
				.WithStringParameter("dayOfWeek")
				.WhenExecute(context => Simple(context, args => SystemFunctions.NextDay(args[0], args[1])))
				.ReturnsDateTime());
		}

		#endregion

		protected override void OnInit() {
			AggregateFunctions();

			ConversionFunctions();
			SecurityFunctions();
			SequenceFunctions();
			DateFunctions();

			MiscFunctions();
		}

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
