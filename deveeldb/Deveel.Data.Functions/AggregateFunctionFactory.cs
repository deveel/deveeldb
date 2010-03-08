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
using System.Collections;

namespace Deveel.Data.Functions {
	internal class AggregateFunctionFacvtory : FunctionFactory {
		public override void Init() {
			AddFunction("aggor", typeof(AggOrFunction), FunctionType.Aggregate);
			AddFunction("avg", typeof (AvgFunction), FunctionType.Aggregate);
			AddFunction("count", typeof (CountFunction), FunctionType.Aggregate);
			AddFunction("max", typeof(MaxFunction), FunctionType.Aggregate);
			AddFunction("min", typeof(MinFunction), FunctionType.Aggregate);
			AddFunction("sum", typeof(SumFunction), FunctionType.Aggregate);
			AddFunction("distinct_count", typeof(DistinctCountFunction), FunctionType.Aggregate);
		}

		#region AggOrFunction

		[Serializable]
		class AggOrFunction : AggregateFunction {
			public AggOrFunction(Expression[] parameters)
				: base("aggor", parameters) {
			}

			protected override TObject EvalAggregate(IGroupResolver group, IQueryContext context, TObject ob1, TObject ob2) {
				// Assuming bitmap numbers, this will find the result of or'ing all the
				// values in the aggregate set.
				if (ob1 != null) {
					if (ob2.IsNull) {
						return ob1;
					} else {
						if (!ob1.IsNull) {
							return ob1.Or(ob2);
						} else {
							return ob2;
						}
					}
				}
				return ob2;
			}

		}


		#endregion

		#region MaxFunction

		[Serializable]
		class MaxFunction : AggregateFunction {
			public MaxFunction(Expression[] parameters)
				: base("max", parameters) {
			}

			protected override TObject EvalAggregate(IGroupResolver group, IQueryContext context, TObject ob1, TObject ob2) {
				// This will find max,
				if (ob1 != null) {
					if (ob2.IsNull) {
						return ob1;
					} else {
						if (!ob1.IsNull && ob1.CompareToNoNulls(ob2) > 0) {
							return ob1;
						} else {
							return ob2;
						}
					}
				}
				return ob2;
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				// Set to return the same type object as this variable.
				return this[0].ReturnTType(resolver, context);
			}
		}


		#endregion

		#region MinFunction

		[Serializable]
		class MinFunction : AggregateFunction {

			public MinFunction(Expression[] parameters)
				: base("min", parameters) {
			}

			protected override TObject EvalAggregate(IGroupResolver group, IQueryContext context,
												  TObject ob1, TObject ob2) {
				// This will find min,
				if (ob1 != null) {
					if (ob2.IsNull) {
						return ob1;
					} else {
						if (!ob1.IsNull && ob1.CompareToNoNulls(ob2) < 0) {
							return ob1;
						} else {
							return ob2;
						}
					}
				}
				return ob2;
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				// Set to return the same type object as this variable.
				return this[0].ReturnTType(resolver, context);
			}

		}


		#endregion

		#region SumFunction

		[Serializable]
		class SumFunction : AggregateFunction {
			public SumFunction(Expression[] parameters)
				: base("sum", parameters) {
			}

			protected override TObject EvalAggregate(IGroupResolver group, IQueryContext context,
												  TObject ob1, TObject ob2) {
				// This will sum,
				if (ob1 != null) {
					if (ob2.IsNull) {
						return ob1;
					} else {
						if (!ob1.IsNull) {
							return ob1.Add(ob2);
						} else {
							return ob2;
						}
					}
				}
				return ob2;
			}
		}

		#endregion

		#region AvgFunction

		[Serializable]
		class AvgFunction : AggregateFunction {
			public AvgFunction(Expression[] parameters)
				: base("avg", parameters) {
			}

			protected override TObject EvalAggregate(IGroupResolver group, IQueryContext context, TObject ob1, TObject ob2) {
				// This will sum,
				if (ob1 != null) {
					if (ob2.IsNull) {
						return ob1;
					} else {
						if (!ob1.IsNull) {
							return ob1.Add(ob2);
						} else {
							return ob2;
						}
					}
				}
				return ob2;
			}

			protected override TObject PostEvalAggregate(IGroupResolver group, IQueryContext context, TObject result) {
				// Find the average from the sum result
				if (result.IsNull) {
					return result;
				}
				return result.Divide(TObject.GetInt4(group.Count));
			}
		}


		#endregion

		#region CountFunction

		[Serializable]
		class CountFunction : Function {
			public CountFunction(Expression[] parameters)
				: base("count", parameters) {
				SetAggregate(true);

				if (ParameterCount != 1)
					throw new Exception("'count' function must have one argument.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
											 IQueryContext context) {
				if (group == null) {
					throw new Exception("'count' can only be used as an aggregate function.");
				}

				int size = group.Count;
				TObject result;
				// if, count(*)
				if (size == 0 || IsGlob) {
					result = TObject.GetInt4(size);
				} else {
					// Otherwise we need to count the number of non-null entries in the
					// columns list(s).

					int total_count = size;

					Expression exp = this[0];
					for (int i = 0; i < size; ++i) {
						TObject val =
							exp.Evaluate(null, group.GetVariableResolver(i), context);
						if (val.IsNull) {
							--total_count;
						}
					}

					result = TObject.GetInt4(total_count);
				}

				return result;
			}

		}


		#endregion

		#region DistinctCountFunction

		[Serializable]
		class DistinctCountFunction : Function {
			public DistinctCountFunction(Expression[] parameters)
				: base("distinct_count", parameters) {
				SetAggregate(true);

				if (ParameterCount <= 0) {
					throw new Exception("'distinct_count' function must have at least one argument.");
				}

			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
											 IQueryContext context) {
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

				if (group == null)
					throw new Exception("'count' can only be used as an aggregate function.");

				int rows = group.Count;
				if (rows <= 1) {
					// If count of entries in group is 0 or 1
					return TObject.GetInt4(rows);
				}

				// Make an array of all cells in the group that we are finding which
				// are distinct.
				int cols = ParameterCount;
				TObject[] group_r = new TObject[rows * cols];
				int n = 0;
				for (int i = 0; i < rows; ++i) {
					IVariableResolver vr = group.GetVariableResolver(i);
					for (int p = 0; p < cols; ++p) {
						Expression exp = this[p];
						group_r[n + p] = exp.Evaluate(null, vr, context);
					}
					n += cols;
				}

				// A comparator that sorts this set,
				IComparer c = new ComparerImpl(cols, group_r);

				// The list of indexes,
				Object[] list = new Object[rows];
				for (int i = 0; i < rows; ++i) {
					list[i] = i;
				}

				// Sort the list,
				Array.Sort(list, c);

				// The count of distinct elements, (there will always be at least 1)
				int distinct_count = 1;
				for (int i = 1; i < rows; ++i) {
					int v = c.Compare(list[i], list[i - 1]);
					// If v == 0 then entry is not distinct with the previous element in
					// the sorted list therefore the distinct counter is not incremented.
					if (v > 0) {
						// If current entry is greater than previous then we've found a
						// distinct entry.
						++distinct_count;
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
					int first_entry = (int)list[0];
					// Assume first is null
					bool first_is_null = true;
					for (int m = 0; m < cols && first_is_null == true; ++m) {
						TObject val = group_r[(first_entry * cols) + m];
						if (!val.IsNull) {
							// First isn't null
							first_is_null = false;
						}
					}
					// Is first NULL?
					if (first_is_null) {
						// decrease distinct count so we don't count the null entry.
						distinct_count = distinct_count - 1;
					}
				}

				return TObject.GetInt4(distinct_count);
			}

			private class ComparerImpl : IComparer {
				private readonly int cols;
				private readonly TObject[] group_r;

				public ComparerImpl(int cols, TObject[] groupR) {
					this.cols = cols;
					group_r = groupR;
				}

				public int Compare(Object ob1, Object ob2) {
					int r1 = (int)ob1;
					int r2 = (int)ob2;

					// Compare row r1 with r2
					int index1 = r1 * cols;
					int index2 = r2 * cols;
					for (int n = 0; n < cols; ++n) {
						int v = group_r[index1 + n].CompareTo(group_r[index2 + n]);
						if (v != 0) {
							return v;
						}
					}

					// If we got here then rows must be equal.
					return 0;
				}
			}
		}

		#endregion
	}
}