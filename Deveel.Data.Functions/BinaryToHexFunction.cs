// 
//  BinaryToHexFunction.cs
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
using System.IO;
using System.Text;

namespace Deveel.Data.Functions {
	internal sealed class BinaryToHexFunction : Function {

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

				return TObject.GetString(buf.ToString());
			} else {
				throw new Exception("'binarytohex' parameter type is not a binary object.");
			}

		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			return TType.StringType;
		}

	}
}