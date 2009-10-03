// 
//  UniqueKeyFunction.cs
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

namespace Deveel.Data.Functions {
	internal sealed class UniqueKeyFunction : Function {

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
			return TObject.GetInt8(v);
		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			return TType.NumericType;
		}

	}
}