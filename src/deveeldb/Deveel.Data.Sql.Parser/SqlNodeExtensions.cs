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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

namespace Deveel.Data.Sql.Parser {
	/// <summary>
	/// Extension methods to <see cref="ISqlNode"/> for diagnostics and other purposes.
	/// </summary>
	public static class SqlNodeExtensions {
		public static TNode FindNode<TNode>(this ISqlNode node) where TNode : class, ISqlNode {
			var foundNode = node.FindNode(typeof(TNode));
			if (foundNode == null)
				return null;

			return (TNode)foundNode;
		}

		public static ISqlNode FindNode(this ISqlNode node, Type nodeType) {
			foreach (var childNode in node.ChildNodes) {
				if (nodeType.IsInstanceOfType(childNode))
					return childNode;

				var foundNode = childNode.FindNode(nodeType);
				if (foundNode != null)
					return foundNode;
			}

			return null;
		}

		public static TNode FindNodeOf<TNode>(this ISqlNode node, string nodeName) where TNode : class, ISqlNode {
			var parent = node.FindByName(nodeName);
			if (parent == null)
				return null;

			return parent.FindNode<TNode>();
		}

		public static ISqlNode FindByName(this ISqlNode node, string nodeName) {
			foreach (var childNode in node.ChildNodes) {
				if (childNode.NodeName.Equals(nodeName))
					return childNode;

				var foundNode = childNode.FindByName(nodeName);
				if (foundNode != null)
					return foundNode;
			}

			return null;
		}

		public static bool HasOptionalNode(this ISqlNode node, string nodeName) {
			var foundNode = node.FindByName(nodeName);
			if (foundNode == null)
				return false;

			return foundNode.ChildNodes.Any();
		}

		public static IEnumerable<TNode> FindNodes<TNode>(this ISqlNode node) where TNode : class, ISqlNode {
			var foundNodes = node.FindNodes(typeof(TNode));
			return foundNodes.Cast<TNode>();
		}

		public static IEnumerable FindNodes(this ISqlNode node, Type nodeType) {
			var nodes = new List<ISqlNode>();

			foreach (var childNode in node.ChildNodes) {
				if (nodeType.IsInstanceOfType(childNode)) {
					nodes.Add(childNode);
				}

				nodes.AddRange(childNode.FindNodes(nodeType).Cast<ISqlNode>());
			}

			return nodes.ToArray();
		}

		public static int StartColumn(this ISqlNode node) {
			if (node == null || node.Tokens == null)
				return 0;

			var token = node.Tokens.FirstOrDefault();
			return token == null ? 0 : token.Column;
		}

		public static int EndColumn(this ISqlNode node) {
			if (node == null || node.Tokens == null)
				return 0;

			var token = node.Tokens.LastOrDefault();
			return token == null ? 0 : token.Column;			
		}

		public static int StartLine(this ISqlNode node) {
			if (node == null || node.Tokens == null)
				return 0;

			var token = node.Tokens.FirstOrDefault();
			return token == null ? 0 : token.Line;
		}

		public static int EndLine(this ISqlNode node) {
			if (node == null || node.Tokens == null)
				return 0;

			var token = node.Tokens.LastOrDefault();
			return token == null ? 0 : token.Line;
		}
	}
}