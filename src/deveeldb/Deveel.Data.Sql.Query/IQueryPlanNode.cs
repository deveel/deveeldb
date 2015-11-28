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

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Query {
	///<summary>
	/// A node element of a query plan tree.
	///</summary>
	/// <remarks>
	/// A plan of a query is represented as a tree structure of such 
	/// nodes. The design allows for plan nodes to be easily reorganized 
	/// for the construction of better plans.
	/// </remarks>
	public interface IQueryPlanNode : ISerializable {
		ITable Evaluate(IRequest context);
	}
}
