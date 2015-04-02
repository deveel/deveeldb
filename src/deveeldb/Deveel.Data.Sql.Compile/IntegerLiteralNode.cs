// 
//  Copyright 2010-2015 Deveel
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
//

using System;
using System.Linq;

using Deveel.Math;

namespace Deveel.Data.Sql.Compile {
	/// <summary>
	/// Encapsulates a number that is any falling in the group of integers. 
	/// </summary>
	/// <remarks>
	/// Nodes of this kind can handle only integer values: the broader handler
	/// node for more complex numeric values is <see cref="NumberLiteralNode"/>.
	/// </remarks>
	[Serializable]
	class IntegerLiteralNode : SqlNode {
		internal BigInteger BigValue { get; private set; }

		/// <summary>
		/// Gets the integer numeric value handled by the node.
		/// </summary>
		public long Value {
			get { return BigValue == null ? -1 : BigValue.ToInt64(); }
		}

		/// <inheritdoc/>
		protected override void OnNodeInit() {
			var token = Tokens.First();
			BigValue = new BigInteger(token.Text);
		}
	}
}