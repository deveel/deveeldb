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

using Deveel.Data.Sql.Objects;
using Deveel.Math;

namespace Deveel.Data.Sql.Parser {
	/// <summary>
	/// Handles a numeric literal value, belonging to a wider group than
	/// integer numbers, spanning from real to decimals.
	/// </summary>
	[Serializable]
	public sealed class NumberLiteralNode : SqlNode {
		internal NumberLiteralNode() {
		}

		internal BigDecimal BigValue { get; private set; }

		/// <summary>
		/// Gets the numeric value handled by the node.
		/// </summary>
		public SqlNumber Value {
			get { return BigValue == null ? SqlNumber.Null : new SqlNumber(BigValue); }
		}

		protected override void OnNodeInit() {
			var token = Tokens.First();
			BigValue = BigDecimal.Parse(token.Text);
		}
	}
}