// 
//  UserFunction.cs
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
	// Returns the user name
	internal sealed class UserFunction : Function {

		public UserFunction(Expression[] parameters)
			: base("user", parameters) {

			if (ParameterCount > 0) {
				throw new Exception("'user' function must have no arguments.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
		                                 IQueryContext context) {
			return TObject.GetString(context.UserName);
		}

		protected override TType ReturnTType() {
			return TType.StringType;
		}

	}
}