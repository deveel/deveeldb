//  
//  RoundFunction.cs
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

using Deveel.Math;

namespace Deveel.Data.Functions {
	internal sealed class RoundFunction : Function {

		public RoundFunction(Expression[] parameters)
			: base("round", parameters) {

			if (ParameterCount < 1 || ParameterCount > 2) {
				throw new Exception("Round function must have one or two arguments.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
		                                 IQueryContext context) {
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
			return TObject.GetBigNumber(v.SetScale(d, DecimalRoundingMode.HalfUp));
		}

	}
}