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
using System.Runtime.Serialization;

using Deveel.Data;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Query {
	/// <summary>
	/// A <see cref="IQueryPlanNode"/> implementation that is a branch with 
	/// two child nodes.
	/// </summary>
	abstract class BranchQueryPlanNode : IQueryPlanNode {
		// The left and right node.

		protected BranchQueryPlanNode(IQueryPlanNode left, IQueryPlanNode right) {
			Left = left;
			Right = right;
		}

		protected BranchQueryPlanNode(SerializationInfo info, StreamingContext context) {
			Left = (IQueryPlanNode)info.GetValue("Left", typeof(IQueryPlanNode));
			Right = (IQueryPlanNode)info.GetValue("Right", typeof(IQueryPlanNode));
		}

		/// <summary>
		/// Gets the left node of the branch query plan node.
		/// </summary>
		public IQueryPlanNode Left { get; private set; }

		/// <summary>
		/// Gets the right node of the branch query plan node.
		/// </summary>
		public IQueryPlanNode Right { get; private set; }

		public abstract ITable Evaluate(IRequest context);

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Left", Left, typeof(IQueryPlanNode));
			info.AddValue("Right", Right, typeof(IQueryPlanNode));

			GetData(info, context);
		}

		protected virtual void GetData(SerializationInfo info, StreamingContext context) {
		}
	}
}