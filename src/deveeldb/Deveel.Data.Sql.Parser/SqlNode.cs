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
using System.Collections.Generic;
using System.Collections.ObjectModel;

using Irony.Ast;
using Irony.Parsing;

namespace Deveel.Data.Sql.Parser {
	/// <summary>
	/// The default implementation of <see cref="ISqlNode"/>, that is a node
	/// in the text analysis parsing of SQL commands.
	/// </summary>
	/// <seealso cref="ISqlNode"/>
	public class SqlNode : ISqlNode,  IAstNodeInit {
		public SqlNode() {
			ChildNodes = new ReadOnlyCollection<ISqlNode>(new ISqlNode[0]);
			Tokens = new ReadOnlyCollection<Token>(new Token[0]);
		}

		/// <summary>
		/// Gets the parent of the current node.
		/// </summary>
		/// <seealso cref="ISqlNode.Parent"/>
		protected ISqlNode Parent { get; private set; }

		/// <summary>
		/// Gets the name of the node, as expressed in the SQL grammar.
		/// </summary>
		/// <seealso cref="ISqlNode.NodeName"/>
		protected string NodeName { get; private set; }

		/// <summary>
		/// Gets an immutable list of <see cref="ISqlNode">nodes</see>, children
		/// of the current node.
		/// </summary>
		/// <seealso cref="ISqlNode.ChildNodes"/>
		protected IEnumerable<ISqlNode> ChildNodes { get; private set; }

		/// <summary>
		/// Gets an immutable list of <see cref="Token"/> that represent the
		/// source of this node.
		/// </summary>
		/// <seealso cref="Token"/>
		/// <seealso cref="ISqlNode.Tokens"/>
		protected IEnumerable<Token> Tokens { get; private set; }

		void IAstNodeInit.Init(AstContext context, ParseTreeNode parseNode) {
			NodeName = parseNode.Term == null ? GetType().Name : parseNode.Term.Name;

			var tokens = new List<Token>();

			var iToken = parseNode.FindToken();
			if (iToken != null) {
				tokens.Add(new Token(iToken.Location.Column, iToken.Location.Line, iToken.Text, iToken.Value));
			}

			var childNodes = new List<ISqlNode>();

			foreach (var childNode in parseNode.ChildNodes) {
				ISqlNode child;
				if (childNode.Term is KeyTerm) {
					var childIToken = childNode.FindToken();
					child = new SqlKeyNode(new Token(childIToken.Location.Column, childIToken.Location.Line, childIToken.Text, childIToken.Value));
				} else {
					child = (ISqlNode)childNode.AstNode;
				}

				child = OnChildNode(child);

				if (child != null) {
					if (child is ISqlChildNode)
						(child as ISqlChildNode).SetParent(this);

					childNodes.Add(child);
					tokens.AddRange(child.Tokens);
				}
			}

			ChildNodes = childNodes.ToArray();
			Tokens = tokens.ToArray();

			OnNodeInit();
		}

		string ISqlNode.NodeName {
			get { return NodeName; }
		}

		ISqlNode ISqlNode.Parent {
			get { return Parent; }
		}

		IEnumerable<ISqlNode> ISqlNode.ChildNodes {
			get { return ChildNodes; }
		}

		IEnumerable<Token> ISqlNode.Tokens {
			get { return Tokens; }
		}

		/// <summary>
		/// After the initialization of the node from the parser, this method
		/// is invoked to let the specific initialization to occur.
		/// </summary>
		protected virtual void OnNodeInit() {
		}

		/// <summary>
		/// During the initialization of the node from the parser, this method
		/// is called for every child node added to <see cref="ChildNodes"/>
		/// </summary>
		/// <param name="node">The node being added to the list of children.</param>
		/// <returns>
		/// Returns a normalized version of the child node, or the node itself.
		/// </returns>
		protected virtual ISqlNode OnChildNode(ISqlNode node) {
			return node;
		}
	}
}