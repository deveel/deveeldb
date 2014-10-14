// 
//  Copyright 2010-2014 Deveel
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

using System;
using System.Linq;

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	public sealed class DataTypeNode : SqlNode {
		public DataTypeNode() {
			IsPrimitive = true;
		}

		public bool IsPrimitive { get; private set; }

		public string TypeName { get; private set; }

		public ObjectName UserTypeName { get; private set; }

		public int Size { get; private set; }

		public bool HasSize { get; private set; }

		public int Scale { get; private set; }

		public bool HasScale { get; private set; }

		public int Precision { get; private set; }

		public bool HasPrecision { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "decimal_type") {
				GetNumberType(node);
			} else if (node.NodeName == "character_type" ||
			           node.NodeName == "binary_type") {
				GetSizedType(node);
			} else if (node.NodeName == "date_type" ||
			           node.NodeName == "integer_type" ||
			           node.NodeName == "float_type") {
				GetSimpleType(node);
			} else if (node.NodeName == "interval_type") {
			} else if (node.NodeName == "user_type") {
				GetUserType(node);
			} else if (node.NodeName == "row_type") {

			}

			return base.OnChildNode(node);
		}

		private void GetUserType(ISqlNode node) {
			IsPrimitive = false;
			UserTypeName = ((ObjectNameNode) node.ChildNodes.First()).Name;
			TypeName = UserTypeName.FullName;
		}

		private void GetSimpleType(ISqlNode node) {
			var type = node.ChildNodes.First();
			TypeName = type.NodeName;
		}

		private void GetSizedType(ISqlNode node) {
			foreach (var childNode in node.ChildNodes) {
				if (childNode.NodeName == "long_varchar") {
					TypeName = "LONG VARCHAR";
				} else if (childNode is SqlKeyNode) {
					TypeName = ((SqlKeyNode) childNode).Text;
				} else if (childNode.NodeName == "datatype_size") {
					GetDataSize(childNode);
				}
			}
		}

		private void GetDataSize(ISqlNode node) {
			foreach (var childNode in node.ChildNodes) {
				if (childNode is IntegerLiteralNode) {
					Size = ((IntegerLiteralNode) childNode).Value;
					HasSize = true;
				}
			}
		}

		private void GetNumberType(ISqlNode node) {
			foreach (var childNode in node.ChildNodes) {
				if (childNode is SqlKeyNode) {
					TypeName = ((SqlKeyNode) childNode).Text;
				} else if (childNode.NodeName == "number_precision") {
					GetNumberPrecision(childNode);
				}
			}

		}

		private void GetNumberPrecision(ISqlNode node) {
			foreach (var childNode in node.ChildNodes) {
				if (!HasScale) {
					Scale = ((IntegerLiteralNode) childNode).Value;
					HasScale = true;
				} else {
					Precision = ((IntegerLiteralNode) childNode).Value;
					HasPrecision = true;
				}
			}
		}
	}
}