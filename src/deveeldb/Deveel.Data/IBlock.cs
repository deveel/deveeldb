// 
//  Copyright 2010-2016 Deveel
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

namespace Deveel.Data {
	/// <summary>
	/// Represents a single block of statements within an execution plan.
	/// </summary>
	/// <remarks>
	/// Blocks provide an isolated context that inherits from parent blocks,
	/// but that is disposed at the end of the execution of the block.
	/// <para>
	/// In an execution plan, a block is a second-level element of execution
	/// within the hierarchical tree: blocks can be parented by a <see cref="IQuery"/>
	/// </para>
	/// <para>
	/// A typical example of a block is the execution body of a <c>LOOP</c> statement,
	/// the PL/SQL block or a procedural code block.
	/// </para>
	/// </remarks>
	public interface IBlock : IRequest {
		/// <summary>
		/// Gets the context of the block
		/// </summary>
		/// <remarks>
		/// A block context manages variables and cursors defined
		/// within that scope, and it's disposed at the end of the
		/// execution of the block.
		/// </remarks>
		new IBlockContext Context { get; }

		/// <summary>
		/// Gets the reference to the block that is parent
		/// to this block in an execution plan.
		/// </summary>
		/// <value>
		/// This value is <c>null</c> if the block is a first-level
		/// block and its parent is a <see cref="IQuery"/>.
		/// </value>
		IBlock Parent { get; }
	}
}
