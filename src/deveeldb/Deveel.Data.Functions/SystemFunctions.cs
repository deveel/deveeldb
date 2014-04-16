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

namespace Deveel.Data.Functions {
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

		#region SystemFunctionsFactory

		class SystemFunctionsFactory : FunctionFactory {
			public override void Init() {
				// Aggregate Functions
				AddFunction("aggor", typeof(AggOrFunction), FunctionType.Aggregate);
				AddFunction("avg", typeof(AvgFunction), FunctionType.Aggregate);
				AddFunction("count", typeof(CountFunction), FunctionType.Aggregate);
				AddFunction("max", typeof(MaxFunction), FunctionType.Aggregate);
				AddFunction("min", typeof(MinFunction), FunctionType.Aggregate);
				AddFunction("sum", typeof(SumFunction), FunctionType.Aggregate);
				AddFunction("distinct_count", typeof(DistinctCountFunction), FunctionType.Aggregate);

				// Arithmetic Functions
				AddFunction("abs", typeof(AbsFunction));
				AddFunction("acos", typeof(ACosFunction));
				AddFunction("asin", typeof(ASinFunction));
				AddFunction("atan", typeof(ATanFunction));
				AddFunction("cos", typeof(CosFunction));
				AddFunction("cosh", typeof(CosHFunction));
				AddFunction("sign", typeof(SignFunction));
				AddFunction("signum", typeof(SignFunction));
				AddFunction("sin", typeof(SinFunction));
				AddFunction("sinh", typeof(SinHFunction));
				AddFunction("sqrt", typeof(SqrtFunction));
				AddFunction("tan", typeof(TanFunction));
				AddFunction("tanh", typeof(TanHFunction));
				AddFunction("mod", typeof(ModFunction));
				AddFunction("pow", typeof(PowFunction));
				AddFunction("round", typeof(RoundFunction));
				AddFunction("log", typeof(LogFunction));
				AddFunction("log10", typeof(Log10Function));
				AddFunction("pi", typeof(PiFunction));
				AddFunction("e", typeof(EFunction));
				AddFunction("ceil", typeof(CeilFunction));
				AddFunction("ceiling", typeof(CeilFunction));
				AddFunction("floor", typeof(FloorFunction));
				AddFunction("radians", typeof(RadiansFunction));
				AddFunction("degrees", typeof(DegreesFunction));
				AddFunction("exp", typeof(ExpFunction));
				AddFunction("cot", typeof(CotFunction));
				AddFunction("arctan", typeof(ArcTanFunction));
				AddFunction("rand", typeof(RandFunction));

				// Date/Time Functions
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

				// String Functions
				AddFunction("concat", typeof(ConcatFunction));
				AddFunction("lower", typeof(LowerFunction));
				AddFunction("tolower", typeof(LowerFunction));
				AddFunction("upper", typeof(UpperFunction));
				AddFunction("toupper", typeof(UpperFunction));
				AddFunction("sql_trim", typeof(SQLTrimFunction));
				AddFunction("ltrim", typeof(LTrimFunction));
				AddFunction("rtrim", typeof(RTrimFunction));
				AddFunction("substring", typeof(SubstringFunction));
				AddFunction("instr", typeof(InStrFunction));
				AddFunction("soundex", typeof(SoundexFunction));
				AddFunction("lpad", typeof(LPadFunction));
				AddFunction("rpad", typeof(RPadFunction));
				AddFunction("replace", typeof(ReplaceFunction));
				AddFunction("char_length", typeof(CharLengthFunction));
				AddFunction("character_length", typeof(CharLengthFunction));
				AddFunction("octet_length", typeof(OctetLengthFunction));

				// Binary Functions
				AddFunction("crc32", typeof(Crc32Function));
				AddFunction("adler32", typeof(Adler32Function));
				AddFunction("compress", typeof(CompressFunction));
				AddFunction("uncompress", typeof(UncompressFunction));

				// Internal Functions
				// Object instantiation (Internal)
				AddFunction("_new_Object", typeof(ObjectInstantiation2));

				// Internal functions
				AddFunction("i_frule_convert", typeof(FRuleConvertFunction));
				AddFunction("i_sql_type", typeof(SQLTypeString));
				AddFunction("i_view_data", typeof(ViewDataConvert));
				AddFunction("i_privilege_string", typeof(PrivilegeString));

				// Casting functions
				AddFunction("tonumber", typeof(ToNumberFunction));
				AddFunction("sql_cast", typeof(SQLCastFunction));
				// Security
				AddFunction("user", typeof(UserFunction), FunctionType.StateBased);
				AddFunction("privgroups", typeof(PrivGroupsFunction), FunctionType.StateBased);
				// Sequence operations
				AddFunction("uniquekey", typeof(UniqueKeyFunction), FunctionType.StateBased);
				AddFunction("nextval", typeof(NextValFunction), FunctionType.StateBased);
				AddFunction("currval", typeof(CurrValFunction), FunctionType.StateBased);
				AddFunction("setval", typeof(SetValFunction), FunctionType.StateBased);
				// Misc
				AddFunction("hextobinary", typeof(HexToBinaryFunction));
				AddFunction("binarytohex", typeof(BinaryToHexFunction));
				// Lists
				AddFunction("least", typeof(LeastFunction));
				AddFunction("greatest", typeof(GreatestFunction));
				// Branch
				AddFunction("if", typeof(IfFunction));
				AddFunction("coalesce", typeof(CoalesceFunction));

				// identity
				AddFunction("identity", typeof(IdentityFunction), FunctionType.StateBased);

				AddFunction("version", typeof(VersionFunction));
				AddFunction("nullif", typeof(NullIfFunction));
				AddFunction("length", typeof(LengthFunction));

				AddFunction("sql_exists", typeof(ExistsFunction));
				AddFunction("sql_unique", typeof(UniqueFunction));

				// crypto
				AddFunction("hash", typeof(HashFunction));
			}
		}

		#endregion

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
				return result.Divide(TObject.CreateInt4(group.Count));
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
					result = TObject.CreateInt4(size);
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

					result = TObject.CreateInt4(total_count);
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

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
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
					return TObject.CreateInt4(rows);
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

				return TObject.CreateInt4(distinct_count);
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

		#region AbsFunction

		[Serializable]
		private sealed class AbsFunction : Function {
			public AbsFunction(Expression[] parameters)
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
		sealed class ACosFunction : Function {
			public ACosFunction(Expression[] parameters)
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
		private sealed class ASinFunction : Function {
			public ASinFunction(Expression[] parameters)
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
		sealed class ATanFunction : Function {
			public ATanFunction(Expression[] parameters)
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
		private class ArcTanFunction : Function {
			public ArcTanFunction(Expression[] parameters)
				: base("arctan", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				double degrees = ob.ToBigNumber().ToDouble();
				double radians = RadiansFunction.ToRadians(degrees);
				radians = System.Math.Tan(System.Math.Atan(radians));
				degrees = DegreesFunction.ToDegrees(radians);

				return TObject.CreateBigNumber(degrees);
			}
		}

		#endregion

		#region CotFunction

		[Serializable]
		private class CotFunction : Function {
			public CotFunction(Expression[] parameters)
				: base("cot", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				double degrees = ob.ToBigNumber().ToDouble();
				double radians = RadiansFunction.ToRadians(degrees);
				double cotan = 1.0 / System.Math.Tan(radians);

				return TObject.CreateBigNumber(cotan);
			}
		}

		#endregion

		#region CosFunction

		[Serializable]
		sealed class CosFunction : Function {
			public CosFunction(Expression[] parameters)
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
		sealed class CosHFunction : Function {
			public CosHFunction(Expression[] parameters)
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
		sealed class SignFunction : Function {
			public SignFunction(Expression[] parameters)
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
		class SinFunction : Function {
			public SinFunction(Expression[] parameters)
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
		class SinHFunction : Function {
			public SinHFunction(Expression[] parameters)
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

		#region SqrtFunction

		[Serializable]
		class SqrtFunction : Function {
			public SqrtFunction(Expression[] parameters)
				: base("sqrt", parameters) {

				if (ParameterCount != 1) {
					throw new Exception("Sqrt function must have one argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}

				return TObject.CreateBigNumber(ob.ToBigNumber().Sqrt());
			}
		}

		#endregion

		#region TanFunction

		[Serializable]
		class TanFunction : Function {
			public TanFunction(Expression[] parameters)
				: base("tan", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(System.Math.Tan(ob.ToBigNumber().ToDouble()));
			}
		}


		#endregion

		#region TanHFunction

		[Serializable]
		class TanHFunction : Function {
			public TanHFunction(Expression[] parameters)
				: base("tanh", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(System.Math.Tanh(ob.ToBigNumber().ToDouble()));
			}
		}


		#endregion

		#region ModFunction

		[Serializable]
		class ModFunction : Function {
			public ModFunction(Expression[] parameters)
				: base("mod", parameters) {

				if (ParameterCount != 2)
					throw new Exception("Mod function must have two arguments.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
											 IQueryContext context) {
				TObject ob1 = this[0].Evaluate(group, resolver, context);
				TObject ob2 = this[1].Evaluate(group, resolver, context);
				if (ob1.IsNull) {
					return ob1;
				} else if (ob2.IsNull) {
					return ob2;
				}

				double v = ob1.ToBigNumber().ToDouble();
				double m = ob2.ToBigNumber().ToDouble();
				return TObject.CreateDouble(v % m);
			}
		}


		#endregion

		#region PowFunction

		[Serializable]
		class PowFunction : Function {
			public PowFunction(Expression[] parameters)
				: base("pow", parameters) {

				if (ParameterCount != 2) {
					throw new Exception("Pow function must have two arguments.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
											 IQueryContext context) {
				TObject ob1 = this[0].Evaluate(group, resolver, context);
				TObject ob2 = this[1].Evaluate(group, resolver, context);
				if (ob1.IsNull) {
					return ob1;
				} else if (ob2.IsNull) {
					return ob2;
				}

				double v = ob1.ToBigNumber().ToDouble();
				double w = ob2.ToBigNumber().ToDouble();
				return TObject.CreateDouble(System.Math.Pow(v, w));
			}
		}

		#endregion

		#region RoundFunction

		[Serializable]
		class RoundFunction : Function {
			public RoundFunction(Expression[] parameters)
				: base("round", parameters) {

				if (ParameterCount < 1 || ParameterCount > 2) {
					throw new Exception("Round function must have one or two arguments.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob1 = this[0].Evaluate(group, resolver, context);
				if (ob1.IsNull) {
					return ob1;
				}

				BigNumber v = ob1.ToBigNumber();
				int d = 0;
				if (ParameterCount == 2) {
					TObject ob2 = this[1].Evaluate(group, resolver, context);
					if (ob2.IsNull) {
						d = 0;
					} else {
						d = ob2.ToBigNumber().ToInt32();
					}
				}
				return TObject.CreateBigNumber(v.SetScale(d, RoundingMode.HalfUp));
			}
		}

		#endregion

		#region LogFunction

		[Serializable]
		private class LogFunction : Function {
			public LogFunction(Expression[] parameters)
				: base("log", parameters) {
				if (ParameterCount > 2)
					throw new ArgumentException("The LOG function accepts 1 or 2 arguments.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				int argc = ParameterCount;

				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				double a = ob.ToBigNumber().ToDouble();
				double newBase = double.NaN;
				if (argc == 2) {
					TObject ob1 = this[1].Evaluate(group, resolver, context);
					if (!ob1.IsNull) {
						if (ob1.TType is TNumericType)
							ob1 = ob.CastTo(PrimitiveTypes.Numeric);

						newBase = ob1.ToBigNumber().ToDouble();
					}
				}

				double result = (argc == 1 ? System.Math.Log(a) : System.Math.Log(a, newBase));
				return TObject.CreateBigNumber(result);
			}
		}

		#endregion

		#region Log10Function

		[Serializable]
		private class Log10Function : Function {
			public Log10Function(Expression[] parameters)
				: base("log10", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				if (ob.TType is TNumericType)
					ob = ob.CastTo(PrimitiveTypes.Numeric);

				return TObject.CreateBigNumber(System.Math.Log10(ob.ToBigNumber().ToDouble()));
			}
		}

		#endregion

		#region PiFunction

		[Serializable]
		private class PiFunction : Function {
			public PiFunction(Expression[] parameters)
				: base("pi", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				return TObject.CreateBigNumber(System.Math.PI);
			}
		}

		#endregion

		#region EFunction

		[Serializable]
		private class EFunction : Function {
			public EFunction(Expression[] parameters)
				: base("e", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				return TObject.CreateBigNumber(System.Math.E);
			}
		}

		#endregion

		#region CeilFunction

		[Serializable]
		private class CeilFunction : Function {
			public CeilFunction(Expression[] parameters)
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
		private class FloorFunction : Function {
			public FloorFunction(Expression[] parameters)
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
		private class RadiansFunction : Function {
			public RadiansFunction(Expression[] parameters)
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
		private class DegreesFunction : Function {
			public DegreesFunction(Expression[] parameters)
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
		private class ExpFunction : Function {
			public ExpFunction(Expression[] parameters)
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
		private class RandFunction : Function {
			public RandFunction(Expression[] parameters)
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
					BigNumber num = (BigNumber)exp_res.Object;
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
				return PrimitiveTypes.VarString;
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
				return PrimitiveTypes.Date;
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
					return (DayOfWeek)ob.ToBigNumber().ToInt32();
				return (DayOfWeek)Enum.Parse(typeof(DayOfWeek), ob.ToStringValue(), true);
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
		private class YearFunction : Function {
			public YearFunction(Expression[] parameters)
				: base("year", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return ob;

				return (TObject)ExtractFunction.ExtractField("year", ob);
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

				return (TObject)ExtractFunction.ExtractField("month", ob);
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

				return (TObject)ExtractFunction.ExtractField("day", ob);
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

				return (TObject)ExtractFunction.ExtractField("hour", ob);
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

				return (TObject)ExtractFunction.ExtractField("minute", ob);
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

				return (TObject)ExtractFunction.ExtractField("second", ob);
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

		#region ConcatFunction

		[Serializable]
		class ConcatFunction : Function {

			public ConcatFunction(Expression[] parameters)
				: base("concat", parameters) {

				if (ParameterCount < 1) {
					throw new Exception("Concat function must have at least one argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				StringBuilder cc = new StringBuilder();

				CultureInfo str_locale = null;
				CollationStrength str_strength = 0;
				CollationDecomposition str_decomposition = 0;
				for (int i = 0; i < ParameterCount; ++i) {
					Expression cur_parameter = this[i];
					TObject ob = cur_parameter.Evaluate(group, resolver, context);
					if (!ob.IsNull) {
						cc.Append(ob.Object.ToString());
						TType type1 = ob.TType;
						if (str_locale == null && type1 is TStringType) {
							TStringType str_type = (TStringType)type1;
							str_locale = str_type.Locale;
							str_strength = str_type.Strength;
							str_decomposition = str_type.Decomposition;
						}
					} else {
						return ob;
					}
				}

				// We inherit the locale from the first string parameter with a locale,
				// or use a default VarString if no locale found.
				TType type;
				if (str_locale != null) {
					type = new TStringType(SqlType.VarChar, -1,
										   str_locale, str_strength, str_decomposition);
				} else {
					type = PrimitiveTypes.VarString;
				}

				return new TObject(type, cc.ToString());
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				// Determine the locale of the first string parameter.
				CultureInfo str_locale = null;
				CollationStrength str_strength = 0;
				CollationDecomposition str_decomposition = 0;
				for (int i = 0; i < ParameterCount && str_locale == null; ++i) {
					TType type = this[i].ReturnTType(resolver, context);
					if (type is TStringType) {
						TStringType str_type = (TStringType)type;
						str_locale = str_type.Locale;
						str_strength = str_type.Strength;
						str_decomposition = str_type.Decomposition;
					}
				}

				if (str_locale != null) {
					return new TStringType(SqlType.VarChar, -1,
										   str_locale, str_strength, str_decomposition);
				} else {
					return PrimitiveTypes.VarString;
				}
			}

		}

		#endregion

		#region LowerFunction

		[Serializable]
		class LowerFunction : Function {
			public LowerFunction(Expression[] parameters)
				: base("lower", parameters) {

				if (ParameterCount != 1) {
					throw new Exception("Lower function must have one argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}
				return new TObject(ob.TType, ob.Object.ToString().ToLower());
			}

			protected override TType ReturnTType() {
				return PrimitiveTypes.VarString;
			}
		}

		#endregion

		#region UpperFunction

		[Serializable]
		class UpperFunction : Function {
			public UpperFunction(Expression[] parameters)
				: base("upper", parameters) {

				if (ParameterCount != 1)
					throw new Exception("Upper function must have one argument.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}
				return new TObject(ob.TType, ob.Object.ToString().ToUpper());
			}

			protected override TType ReturnTType() {
				return PrimitiveTypes.VarString;
			}

		}

		#endregion

		#region SQLTrimFunction

		[Serializable]
		class SQLTrimFunction : Function {

			public SQLTrimFunction(Expression[] parameters)
				: base("sql_trim", parameters) {

				//      Console.Out.WriteLine(parameterCount());
				if (ParameterCount != 3) {
					throw new Exception(
						"SQL Trim function must have three parameters.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				// The type of trim (leading, both, trailing)
				TObject ttype = this[0].Evaluate(group, resolver, context);
				// Characters to trim
				TObject cob = this[1].Evaluate(group, resolver, context);
				if (cob.IsNull) {
					return cob;
				} else if (ttype.IsNull) {
					return TObject.CreateString((StringObject)null);
				}
				String characters = cob.Object.ToString();
				String ttype_str = ttype.Object.ToString();
				// The content to trim.
				TObject ob = this[2].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}
				String str = ob.Object.ToString();

				int skip = characters.Length;
				// Do the trim,
				if (ttype_str.Equals("leading") || ttype_str.Equals("both")) {
					// Trim from the start.
					int scan = 0;
					while (scan < str.Length &&
						   str.IndexOf(characters, scan) == scan) {
						scan += skip;
					}
					str = str.Substring(System.Math.Min(scan, str.Length));
				}
				if (ttype_str.Equals("trailing") || ttype_str.Equals("both")) {
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

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.VarString;
			}

		}

		#endregion

		#region LTrimFunction

		[Serializable]
		sealed class LTrimFunction : Function {
			public LTrimFunction(Expression[] parameters)
				: base("ltrim", parameters) {

				if (ParameterCount != 1)
					throw new Exception("ltrim function may only have 1 parameter.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
											 IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}
				String str = ob.Object.ToString();

				// Do the trim,
				// Trim from the start.
				int scan = 0;
				while (scan < str.Length &&
					   str.IndexOf(' ', scan) == scan) {
					scan += 1;
				}
				str = str.Substring(System.Math.Min(scan, str.Length));

				return TObject.CreateString(str);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.VarString;
			}

		}

		#endregion

		#region RTrimFunction

		[Serializable]
		class RTrimFunction : Function {

			public RTrimFunction(Expression[] parameters)
				: base("rtrim", parameters) {

				if (ParameterCount != 1) {
					throw new Exception("rtrim function may only have 1 parameter.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
											 IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}
				String str = ob.Object.ToString();

				// Do the trim,
				// Trim from the end.
				int scan = str.Length - 1;
				int i = str.LastIndexOf(" ", scan);
				while (scan >= 0 && i != -1 && i == scan - 2) {
					scan -= 1;
					i = str.LastIndexOf(" ", scan);
				}
				str = str.Substring(0, System.Math.Max(0, scan + 1));

				return TObject.CreateString(str);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.VarString;
			}

		}

		#endregion

		#region SubstringFunction

		[Serializable]
		class SubstringFunction : Function {
			public SubstringFunction(Expression[] parameters)
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
		private class InStrFunction : Function {
			public InStrFunction(Expression[] parameters)
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

		#region SoundexFunction

		[Serializable]
		class SoundexFunction : Function {
			public SoundexFunction(Expression[] parameters)
				: base("soundex", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject obj = this[0].Evaluate(group, resolver, context);

				if (!(obj.TType is TStringType))
					obj = obj.CastTo(PrimitiveTypes.VarString);

				return TObject.CreateString(Soundex.UsEnglish.Compute(obj.ToStringValue()));
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.VarString;
			}
		}

		#endregion

		#region LPadFunction

		[Serializable]
		private class LPadFunction : Function {
			public LPadFunction(Expression[] parameters)
				: base("lpad", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				int argc = ParameterCount;

				TObject ob1 = this[0].Evaluate(group, resolver, context);
				TObject ob2 = this[1].Evaluate(group, resolver, context);

				if (ob1.IsNull)
					return ob1;

				char c = ' ';
				if (argc > 2) {
					TObject ob3 = this[2].Evaluate(group, resolver, context);
					if (!ob3.IsNull) {
						string pad = ob3.ToStringValue();
						c = pad[0];
					}
				}

				int totalWidth = ob2.ToBigNumber().ToInt32();
				string s = ob1.ToStringValue();

				string result = (argc == 1 ? s.PadLeft(totalWidth) : s.PadLeft(totalWidth, c));
				return TObject.CreateString(result);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.VarString;
			}
		}

		#endregion

		#region RPadFunction

		[Serializable]
		private class RPadFunction : Function {
			public RPadFunction(Expression[] parameters)
				: base("rpad", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				int argc = ParameterCount;

				TObject ob1 = this[0].Evaluate(group, resolver, context);
				TObject ob2 = this[1].Evaluate(group, resolver, context);

				if (ob1.IsNull)
					return ob1;

				char c = ' ';
				if (argc > 2) {
					TObject ob3 = this[2].Evaluate(group, resolver, context);
					if (!ob3.IsNull) {
						string pad = ob3.ToStringValue();
						c = pad[0];
					}
				}

				int totalWidth = ob2.ToBigNumber().ToInt32();
				string s = ob1.ToStringValue();

				string result = (argc == 1 ? s.PadRight(totalWidth) : s.PadRight(totalWidth, c));
				return TObject.CreateString(result);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.VarString;
			}
		}

		#endregion

		#region ReplaceFunction

		[Serializable]
		private class ReplaceFunction : Function {
			public ReplaceFunction(Expression[] parameters)
				: base("replace", parameters) {
				if (ParameterCount != 3)
					throw new ArgumentException("The function REPLACE requires 3 parameters.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob1 = this[0].Evaluate(group, resolver, context);
				TObject ob2 = this[1].Evaluate(group, resolver, context);
				TObject ob3 = this[2].Evaluate(group, resolver, context);

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

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.VarString;
			}
		}

		#endregion

		#region CharLengthFunction

		[Serializable]
		private class CharLengthFunction : Function {
			public CharLengthFunction(Expression[] parameters)
				: base("char_length", parameters) {
			}

			#region Overrides of Function

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (!(ob.TType is TStringType) || ob.IsNull)
					return TObject.Null;

				IStringAccessor s = (IStringAccessor)ob.Object;
				if (s == null)
					return TObject.Null;

				return (TObject)s.Length;
			}

			#endregion
		}

		#endregion

		#region OctetLengthFunction

		[Serializable]
		private class OctetLengthFunction : Function {
			public OctetLengthFunction(Expression[] parameters)
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

		#region FRuleConvertFunction

		// Used in the 'GetxxxKeys' methods in DeveelDbConnection.GetSchema to convert 
		// the update delete rule of a foreign key to the short enum.
		[Serializable]
		class FRuleConvertFunction : Function {
			public FRuleConvertFunction(Expression[] parameters)
				: base("i_frule_convert", parameters) {

				if (ParameterCount != 1)
					throw new Exception("i_frule_convert function must have one argument.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				// The parameter should be a variable reference that is resolved
				TObject ob = this[0].Evaluate(group, resolver, context);

				if (ob.TType is TStringType) {
					String str = null;
					if (!ob.IsNull) {
						str = ob.Object.ToString();
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
				if (ob.TType is TNumericType) {
					var code = ob.ToBigNumber().ToInt32();
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

		}

		#endregion

		#region SQLTypeString

		// Used to form an SQL type string that describes the SQL type and any
		// size/scale information together with it.
		[Serializable]
		class SQLTypeString : Function {
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

		#region ViewDataConvert

		// Used to convert view data in the system view table to forms that are
		// human understandable.  Useful function for debugging or inspecting views.
		[Serializable]
		class ViewDataConvert : Function {

			public ViewDataConvert(Expression[] parameters)
				: base("i_view_data", parameters) {

				if (ParameterCount != 2)
					throw new Exception("i_view_data function must have two arguments.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				// Get the parameters.  The first is a string describing the operation.
				// The second is the binary data to process and output the information
				// for.
				TObject commandObj = this[0].Evaluate(group, resolver, context);
				TObject data = this[1].Evaluate(group, resolver, context);

				String command_str = commandObj.Object.ToString();
				ByteLongObject blob = (ByteLongObject)data.Object;

				if (String.Compare(command_str, "referenced tables", true) == 0) {
					View view = View.DeserializeFromBlob(blob);
					IQueryPlanNode node = view.QueryPlanNode;
					IList<TableName> touched_tables = node.DiscoverTableNames(new List<TableName>());
					StringBuilder buf = new StringBuilder();
					int sz = touched_tables.Count;
					for (int i = 0; i < sz; ++i) {
						buf.Append(touched_tables[i]);
						if (i < sz - 1) {
							buf.Append(", ");
						}
					}
					return TObject.CreateString(buf.ToString());
				} else if (String.Compare(command_str, "plan dump", true) == 0) {
					View view = View.DeserializeFromBlob(blob);
					IQueryPlanNode node = view.QueryPlanNode;
					StringBuilder buf = new StringBuilder();
					node.DebugString(0, buf);
					return TObject.CreateString(buf.ToString());
				} else if (String.Compare(command_str, "query string", true) == 0) {
					SqlQuery query = SqlQuery.DeserializeFromBlob(blob);
					return TObject.CreateString(query.ToString());
				}

				return TObject.Null;

			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.VarString;
			}

		}

		#endregion

		#region ObjectInstantiation

		// Instantiates a new object.
		[Serializable]
		class ObjectInstantiation : Function {
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
		class ObjectInstantiation2 : Function {
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
		class PrivilegeString : Function {

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
		private class Crc32Function : Function {
			public Crc32Function(Expression[] parameters)
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
		private class Adler32Function : Function {
			public Adler32Function(Expression[] parameters)
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
		private class CompressFunction : Function {
			public CompressFunction(Expression[] parameters)
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
		private class UncompressFunction : Function {
			public UncompressFunction(Expression[] parameters)
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
		class ToNumberFunction : Function {
			public ToNumberFunction(Expression[] parameters)
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

		#region IfFunction

		// Conditional - IF(a < 0, NULL, a)
		[Serializable]
		class IfFunction : Function {
			public IfFunction(Expression[] parameters)
				: base("if", parameters) {
				if (ParameterCount != 3) {
					throw new Exception(
						"IF function must have exactly three arguments.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject res = this[0].Evaluate(group, resolver, context);
				if (res.TType is TBooleanType) {
					// Does the result equal true?
					if (res.CompareTo(TObject.CreateBoolean(true)) == 0) {
						// Resolved to true so evaluate the first argument
						return this[1].Evaluate(group, resolver, context);
					} else {
						// Otherwise result must evaluate to NULL or false, so evaluate
						// the second parameter
						return this[2].Evaluate(group, resolver, context);
					}
				}
				// Result was not a bool so return null
				return TObject.Null;
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				// It's impossible to know the return type of this function until runtime
				// because either comparator could be returned.  We could assume that
				// both branch expressions result in the same type of object but this
				// currently is not enforced.

				// Returns type of first argument
				TType t1 = this[1].ReturnTType(resolver, context);
				// This is a hack for null values.  If the first parameter is null
				// then return the type of the second parameter which hopefully isn't
				// also null.
				if (t1 is TNullType) {
					return this[2].ReturnTType(resolver, context);
				}
				return t1;
			}
		}

		#endregion

		#region IdentityFunction

		sealed class IdentityFunction : Function {
			public IdentityFunction(Expression[] parameters)
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

		#region UserFunction

		// Returns the user name
		[Serializable]
		class UserFunction : Function {
			public UserFunction(Expression[] parameters)
				: base("user", parameters) {

				if (ParameterCount > 0) {
					throw new Exception("'user' function must have no arguments.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
											 IQueryContext context) {
				return TObject.CreateString(context.UserName);
			}

			protected override TType ReturnTType() {
				return PrimitiveTypes.VarString;
			}
		}

		#endregion

		#region PrivGroupsFunction

		// Returns the comma (",") deliminated priv groups the user belongs to.
		[Serializable]
		class PrivGroupsFunction : Function {
			public PrivGroupsFunction(Expression[] parameters)
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
		class BinaryToHexFunction : Function {

			readonly static char[] digits = {
		                                	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
		                                	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
		                                	'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
		                                	'u', 'v', 'w', 'x', 'y', 'z'
		                                };

			public BinaryToHexFunction(Expression[] parameters)
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
		class HexToBinaryFunction : Function {
			public HexToBinaryFunction(Expression[] parameters)
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

		#region LeastFunction

		[Serializable]
		class LeastFunction : Function {
			public LeastFunction(Expression[] parameters)
				: base("least", parameters) {

				if (ParameterCount < 1)
					throw new Exception("Least function must have at least 1 argument.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject least = null;
				for (int i = 0; i < ParameterCount; ++i) {
					TObject ob = this[i].Evaluate(group, resolver, context);
					if (ob.IsNull) {
						return ob;
					}
					if (least == null || ob.CompareTo(least) < 0) {
						least = ob;
					}
				}
				return least;
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return this[0].ReturnTType(resolver, context);
			}

		}

		#endregion

		#region GreatestFunction

		[Serializable]
		class GreatestFunction : Function {
			public GreatestFunction(Expression[] parameters)
				: base("greatest", parameters) {

				if (ParameterCount < 1) {
					throw new Exception("Greatest function must have at least 1 argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
											 IQueryContext context) {
				TObject great = null;
				for (int i = 0; i < ParameterCount; ++i) {
					TObject ob = this[i].Evaluate(group, resolver, context);
					if (ob.IsNull) {
						return ob;
					}
					if (great == null || ob.CompareTo(great) > 0) {
						great = ob;
					}
				}
				return great;
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return this[0].ReturnTType(resolver, context);
			}
		}

		#endregion

		#region CoalesceFunction

		// Coalesce - COALESCE(address2, CONCAT(city, ', ', state, '  ', zip))
		[Serializable]
		class CoalesceFunction : Function {
			public CoalesceFunction(Expression[] parameters)
				: base("coalesce", parameters) {
				if (ParameterCount < 1) {
					throw new Exception("COALESCE function must have at least 1 parameter.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				int count = ParameterCount;
				for (int i = 0; i < count - 1; ++i) {
					TObject res = this[i].Evaluate(group, resolver, context);
					if (!res.IsNull) {
						return res;
					}
				}
				return this[count - 1].Evaluate(group, resolver, context);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				// It's impossible to know the return type of this function until runtime
				// because either comparator could be returned.  We could assume that
				// both branch expressions result in the same type of object but this
				// currently is not enforced.

				// Go through each argument until we find the first parameter we can
				// deduce the class of.
				int count = ParameterCount;
				for (int i = 0; i < count; ++i) {
					TType t = this[i].ReturnTType(resolver, context);
					if (!(t is TNullType)) {
						return t;
					}
				}
				// Can't work it out so return null type
				return PrimitiveTypes.Null;
			}

		}

		#endregion

		#region CurrValFunction

		[Serializable]
		class CurrValFunction : Function {
			public CurrValFunction(Expression[] parameters)
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
		class NextValFunction : Function {
			public NextValFunction(Expression[] parameters)
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
		class SetValFunction : Function {
			public SetValFunction(Expression[] parameters)
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
		class UniqueKeyFunction : Function {
			public UniqueKeyFunction(Expression[] parameters)
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

		#region SQLCastFunction

		[Serializable]
		class SQLCastFunction : Function {

			private readonly TType cast_to_type;

			public SQLCastFunction(Expression[] parameters)
				: base("sql_cast", parameters) {

				// Two parameters - the value to cast and the type to cast to (encoded)
				if (ParameterCount != 2) {
					throw new Exception("'sql_cast' function must have only 2 arguments.");
				}

				// Get the encoded type and parse it into a TType object and cache
				// locally in this object.  We expect that the second parameter of this
				// function is always constant.
				Expression exp = parameters[1];
				if (exp.Count != 1) {
					throw new Exception(
						"'sql_cast' function must have simple second parameter.");
				}

				Object vob = parameters[1].Last;
				if (vob is TObject) {
					TObject ob = (TObject)vob;
					String encoded_type = ob.Object.ToString();
					cast_to_type = TType.DecodeString(encoded_type);
				} else {
					throw new Exception("'sql_cast' function must have simple second parameter.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				// If types are the same then no cast is necessary and we return this
				// object.
				if (ob.TType.SqlType == cast_to_type.SqlType) {
					return ob;
				}
				// Otherwise cast the object and return the new typed object.
				Object casted_ob = TType.CastObjectToTType(ob.Object, cast_to_type);
				return new TObject(cast_to_type, casted_ob);

			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return cast_to_type;
			}

		}

		#endregion

		#region VersionFunction

		[Serializable]
		private class VersionFunction : Function {
			public VersionFunction(Expression[] parameters)
				: base("version", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				Version version = ProductInfo.Current.Version;
				return TObject.CreateString(version.ToString(2));
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.VarString;
			}
		}

		#endregion

		#region NullIfFunction

		[Serializable]
		private class NullIfFunction : Function {
			public NullIfFunction(Expression[] parameters)
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

		#region LengthFunction

		[Serializable]
		class LengthFunction : Function {
			public LengthFunction(Expression[] parameters)
				: base("length", parameters) {

				if (ParameterCount != 1)
					throw new Exception("Length function must have one argument.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull) {
					return ob;
				}
				if (ob.TType is TBinaryType) {
					IBlobAccessor blob = (IBlobAccessor)ob.Object;
					return TObject.CreateInt4(blob.Length);
				}
				if (ob.TType is TStringType) {
					IStringAccessor str = (IStringAccessor)ob.Object;
					return TObject.CreateInt4(str.Length);
				}
				return TObject.CreateInt4(ob.Object.ToString().Length);
			}

		}

		#endregion

		#region ExistsFunction

		[Serializable]
		private class ExistsFunction : Function {
			public ExistsFunction(Expression[] parameters)
				: base("sql_exists", parameters) {
			}

			#region Overrides of Function

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.CreateBoolean(false);

				if (!(ob.TType is TQueryPlanType))
					throw new InvalidOperationException("The EXISTS function must have a query argument.");

				IQueryPlanNode plan = ob.Object as IQueryPlanNode;
				if (plan == null)
					throw new InvalidOperationException();

				Table table = plan.Evaluate(context);
				return (TObject)(table.RowCount > 0);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return PrimitiveTypes.Boolean;
			}

			#endregion
		}

		#endregion

		#region UniqueFunction

		[Serializable]
		private class UniqueFunction : Function {
			public UniqueFunction(Expression[] parameters)
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

		class HashFunction : Function {
			public HashFunction(Expression[] parameters)
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