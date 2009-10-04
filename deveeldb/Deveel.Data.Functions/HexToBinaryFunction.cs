//  
//  HexToBinaryFunction.cs
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
	internal sealed class HexToBinaryFunction : Function {
		public HexToBinaryFunction(Expression[] parameters)
			: base("hextobinary", parameters) {

			// One parameter - our hex string.
			if (ParameterCount != 1) {
				throw new Exception(
					"'hextobinary' function must have only 1 argument.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			String str = this[0].Evaluate(group, resolver, context).Object.ToString();

			int str_len = str.Length;
			if (str_len == 0) {
				return new TObject(TType.BinaryType, new ByteLongObject(new byte[0]));
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

			return new TObject(TType.BinaryType, new ByteLongObject(buf));
		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			return TType.BinaryType;
		}

	}
}