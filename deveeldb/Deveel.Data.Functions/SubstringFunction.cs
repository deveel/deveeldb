//  
//  SubstringFunction.cs
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
	internal sealed class SubstringFunction : Function {

		public SubstringFunction(Expression[] parameters)
			: base("substring", parameters) {

			if (ParameterCount < 1 || ParameterCount > 3) {
				throw new Exception(
					"Substring function needs one to three arguments.");
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
				return TObject.GetString("");
			}
			if (arg2 + arg1 > str_length) {
				arg2 = (str_length - arg1) + 1;
			}
			if (arg2 < 1) {
				return TObject.GetString("");
			}

			//TODO: check this...
			return TObject.GetString(str.Substring(arg1 - 1, (arg1 + arg2) - 1));
		}

		protected override TType ReturnTType() {
			return TType.StringType;
		}

	}
}