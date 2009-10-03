// 
//  ConcatFunction.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Globalization;
using System.Text;

using Deveel.Data.Text;

namespace Deveel.Data.Functions {
	internal sealed class ConcatFunction : Function {

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
			// or use a default StringType if no locale found.
			TType type;
			if (str_locale != null) {
				type = new TStringType(SQLTypes.VARCHAR, -1,
				                       str_locale, str_strength, str_decomposition);
			} else {
				type = TType.StringType;
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
				return new TStringType(SQLTypes.VARCHAR, -1,
				                       str_locale, str_strength, str_decomposition);
			} else {
				return TType.StringType;
			}
		}

	}
}