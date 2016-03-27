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

			Register(new DistinctCountFucntion());
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

		class DistinctCountFucntion : Function {
			public DistinctCountFucntion()
				: base(new ObjectName(SystemSchema.SchemaName, "distinct_count"),
					new[] {new RoutineParameter("args", Function.DynamicType, ParameterAttributes.Unbounded)}, PrimitiveTypes.Integer(),
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
