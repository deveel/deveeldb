//  
//  InternalFunctionFactory.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

namespace Deveel.Data.Functions {
	/// <summary>
	/// A <see cref="FunctionFactory"/> for all internal SQL functions 
	/// (including aggregate, mathematical, string functions).
	/// </summary>
	/// <remarks>
	/// This <see cref="FunctionFactory"/> is registered with the 
	/// <see cref="DatabaseSystem"/> during initialization.
	/// </remarks>
	sealed class InternalFunctionFactory : FunctionFactory {
		public override void Init() {

			// Parses a date/time/timestamp string
			AddFunction("dateob", typeof(DateObFunction));
			AddFunction("timeob", typeof(TimeObFunction));
			AddFunction("timestampob", typeof(TimeStampObFunction));
			AddFunction("dateformat", typeof(DateFormatFunction));

			// Casting functions
			AddFunction("tonumber", typeof(ToNumberFunction));
			AddFunction("sql_cast", typeof(SQLCastFunction));
			// String functions
			AddFunction("lower", typeof(LowerFunction));
			AddFunction("upper", typeof(UpperFunction));
			AddFunction("concat", typeof(ConcatFunction));
			AddFunction("length", typeof(LengthFunction));
			AddFunction("substring", typeof(SubstringFunction));
			AddFunction("sql_trim", typeof(SQLTrimFunction));
			AddFunction("ltrim", typeof(LTrimFunction));
			AddFunction("rtrim", typeof(RTrimFunction));
			AddFunction("soundex", typeof (SoundexFunction));
			// Security
			AddFunction("user", typeof(UserFunction));
			AddFunction("privgroups", typeof(PrivGroupsFunction));
			// Aggregate
			AddFunction("count", typeof(CountFunction), FunctionType.Aggregate);
			AddFunction("distinct_count", typeof(DistinctCountFunction), FunctionType.Aggregate);
			AddFunction("avg", typeof(AvgFunction), FunctionType.Aggregate);
			AddFunction("sum", typeof(SumFunction), FunctionType.Aggregate);
			AddFunction("min", typeof(MinFunction), FunctionType.Aggregate);
			AddFunction("max", typeof(MaxFunction), FunctionType.Aggregate);
			AddFunction("aggor", typeof(AggOrFunction), FunctionType.Aggregate);
			// Mathematical
			AddFunction("abs", typeof(AbsFunction));
			AddFunction("sign", typeof(SignFunction));
			AddFunction("mod", typeof(ModFunction));
			AddFunction("round", typeof(RoundFunction));
			AddFunction("pow", typeof(PowFunction));
			AddFunction("sqrt", typeof(SqrtFunction));
			AddFunction("cos", typeof (CosFunction));
			AddFunction("cosh", typeof (CosHFunction));
			AddFunction("acos", typeof (ACosFunction));
			AddFunction("tan", typeof (TanFunction));
			AddFunction("tanh", typeof (TanHFunction));
			AddFunction("sin", typeof (SinFunction));
			AddFunction("sinh", typeof (SinHFunction));
			AddFunction("asin", typeof (ASinFunction));
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
			AddFunction("identity", typeof (IdentityFunction));

			// Object instantiation (Internal)
			AddFunction("_new_Object", typeof(ObjectInstantiation2));

			// Internal functions
			AddFunction("i_frule_convert", typeof(ForeignRuleConvert));
			AddFunction("i_sql_type", typeof(SQLTypeString));
			AddFunction("i_view_data", typeof(ViewDataConvert));
			AddFunction("i_privilege_string", typeof(PrivilegeString));

		}
	}
}