// 
//  PrivilegeString.cs
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

using Deveel.Math;

namespace Deveel.Data.Functions {
	// Given a priv_bit number (from SYS_INFO.sUSRGrant), this will return a
	// text representation of the privilege.
	internal sealed class PrivilegeString : Function {

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
			return TObject.GetString(privs.ToString());
		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			return TType.StringType;
		}
	}
}