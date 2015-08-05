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
using System.IO;
using System.Text;

using Deveel.Data.DbSystem;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;

using DryIoc;

namespace Deveel.Data.Sql.Query {
	public static class QueryPlanSerializers {
		public static readonly IObjectSerializerResolver Resolver = new QueryPlanNodeSerializerResolver();

		public static void Serialize(IQueryPlanNode queryPlan, Stream outputStream) {
			using (var writer = new BinaryWriter(outputStream, Encoding.Unicode)) {
				Serialize(queryPlan, writer);
			}
		}

		public static void Serialize(IQueryPlanNode queryPlan, BinaryWriter writer) {
			var nodeType = queryPlan.GetType();
			var seriializer = Resolver.ResolveSerializer(nodeType) as IObjectBinarySerializer;
			if (seriializer == null)
				throw new InvalidOperationException(String.Format("Could not find any serializer for type '{0}'.", nodeType));

			seriializer.Serialize(queryPlan, writer);			
		}

		public static IQueryPlanNode Deserialize(Type nodeType, Stream inputStream) {
			var seriializer = Resolver.ResolveSerializer(nodeType);
			if (seriializer == null)
				throw new InvalidOperationException(String.Format("Could not find any serializer for type '{0}'.", nodeType));

			return seriializer.Deserialize(inputStream) as IQueryPlanNode;
		}

		public static IQueryPlanNode Deserialize(Type nodeType, BinaryReader reader) {
			var seriializer = Resolver.ResolveSerializer(nodeType) as IObjectBinarySerializer;
			if (seriializer == null)
				throw new InvalidOperationException(String.Format("Could not find any serializer for type '{0}'.", nodeType));

			return (IQueryPlanNode) seriializer.Deserialize(reader);
		}

		private static void WriteChildNode(BinaryWriter writer, IQueryPlanNode node) {
			if (node == null) {
				writer.Write((byte) 0);
			} else {
				var nodeTypeString = node.GetType().FullName;
				writer.Write((byte)1);
				writer.Write(nodeTypeString);

				Serialize(node, writer);
			}
		}

		private static IQueryPlanNode ReadChildNode(BinaryReader reader) {
			var state = reader.ReadByte();
			if (state == 0)
				return null;

			var typeString = reader.ReadString();
#if PCL
			var type = Type.GetType(typeString, true);
#else
			var type = Type.GetType(typeString, true, true);
#endif

			return Deserialize(type, reader);
		}

		private static void WriteArray<T>(T[] arrary, BinaryWriter writer, Action<T, BinaryWriter> write) {
			var count = arrary == null ? 0 : arrary.Length;
			writer.Write(count);

			if (arrary != null) {
				for (int i = 0; i < count; i++) {
					write(arrary[i], writer);
				}
			}
		}

		private static void WriteObjectNames(ObjectName[] names, BinaryWriter writer) {
			WriteArray(names, writer, ObjectName.Serialize);
		}

		private static void WriteExpressions(SqlExpression[] expressions, BinaryWriter writer) {
			WriteArray(expressions, writer, (expression, binaryWriter) => SqlExpression.Serialize(expression, binaryWriter));
		}

		private static void WriteStrings(string[] array, BinaryWriter writer) {
			WriteArray(array, writer, (s, binaryWriter) => binaryWriter.Write(s));
		}

		private static T[] ReadArray<T>(BinaryReader reader, Func<BinaryReader, T> read) {
			var count = reader.ReadInt32();
			var list = new List<T>(count);
			for (int i = 0; i < count; i++) {
				var obj = read(reader);
				list.Add(obj);
			}

			return list.ToArray();
		}

		private static ObjectName[] ReadObjectNames(BinaryReader reader) {
			return ReadArray(reader, ObjectName.Deserialize);
		}

		private static SqlExpression[] ReadExpressions(BinaryReader reader) {
			return ReadArray(reader, binaryReader => SqlExpression.Deserialize(reader));
		}

		private static string[] ReadStrings(BinaryReader reader) {
			return ReadArray(reader, binaryReader => binaryReader.ReadString());
		}

		#region QueryPlanNodeSerializerResolver

		class QueryPlanNodeSerializerResolver : ObjectSerializerProvider {
			protected override void Init() {
				Register<CachePointNode, CacheNodePointSerializer>();
				Register<CompositeNode, CompositeNodeSerializer>();
				Register<ConstantSelectNode, ConstantSelectNodeSerializer>();
				Register<CreateFunctionsNode, CreateFunctionNodeSerializer>();
				Register<DistinctNode, DistinctNodeSerializer>();
				Register<EquiJoinNode, EquiJoinNodeSerializer>();
				Register<ExhaustiveSelectNode, ExhaustiveSelectNodeSerializer>();
				Register<FetchTableNode, FetchTableNodeSerializer>();
				Register<FetchViewNode, FetchViewNodeSerializer>();
				Register<GroupNode, GroupNodeSerializer>();
				Register<JoinNode, JoinNodeSerializer>();
				Register<LeftOuterJoinNode, LeftOuterJoinNodeSerializer>();
				Register<LogicalUnionNode, LogicalUnionNodeSerializer>();
				Register<MarkerNode, MarkerNodeSerializer>();
				Register<NaturalJoinNode, NaturalJoinNodeSerializer>();
				Register<NonCorrelatedAnyAllNode, NonCorrelatedAnyAllNodeSerializer>();
				Register<RangeSelectNode, RageSelectNodeSerializer>();
				Register<SimplePatternSelectNode, SimplePatternSelectNodeSerializer>();
				Register<SimpleSelectNode, SimpleSelectNodeSerializer>();
				Register<SingleRowTableNode, SingleRowTableNodeSerializer>();
				Register<SortNode, SortNodeSerializer>();
				Register<SubsetNode, SubsetNodeSerializer>();
			}
		}

		#endregion

		#region QueryPlanNodeSerializer

		abstract class QueryPlanNodeSerializer<TNode> : ObjectBinarySerializer<TNode> where TNode : class, IQueryPlanNode {
		}

		#endregion

		#region CacheNodePointSerializer

		class CacheNodePointSerializer : QueryPlanNodeSerializer<CachePointNode> {
			public override void Serialize(CachePointNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				writer.Write(node.Id);
			}

			public override CachePointNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var id = reader.ReadInt64();
				return new CachePointNode(child, id);
			}
		}

		#endregion

		#region CompositeNodeSerializer

		class CompositeNodeSerializer : QueryPlanNodeSerializer<CompositeNode> {
			public override void Serialize(CompositeNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Left);
				WriteChildNode(writer, node.Right);
				
				writer.Write(node.All);
				writer.Write((byte)node.CompositeFunction);
			}

			public override CompositeNode Deserialize(BinaryReader reader) {
				var left = ReadChildNode(reader);
				var right = ReadChildNode(reader);
				bool all = reader.ReadBoolean();
				var function = (CompositeFunction) reader.ReadByte();

				return new CompositeNode(left, right, function, all);
			}
		}

		#endregion

		#region ConstantSelectNodeSerializer

		class ConstantSelectNodeSerializer : QueryPlanNodeSerializer<ConstantSelectNode> {
			public override void Serialize(ConstantSelectNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				SqlExpression.Serialize(node.Expression, writer);
			}

			public override ConstantSelectNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var expression = SqlExpression.Deserialize(reader);

				return new ConstantSelectNode(child, expression);
			}
		}

		#endregion

		#region CreateFunctionNodeSerializer

		class CreateFunctionNodeSerializer : QueryPlanNodeSerializer<CreateFunctionsNode> {
			public override void Serialize(CreateFunctionsNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				WriteExpressions(node.Functions, writer);
				WriteStrings(node.Names, writer);				
			}

			public override CreateFunctionsNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);

				var functions = ReadExpressions(reader);
				var names = ReadStrings(reader);

				return new CreateFunctionsNode(child, functions, names);
			}
		}

		#endregion

		#region DistinctNodeSerializer

		class DistinctNodeSerializer : QueryPlanNodeSerializer<DistinctNode> {
			public override void Serialize(DistinctNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				WriteObjectNames(node.ColumnNames, writer);
			}

			public override DistinctNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var names = ReadObjectNames(reader);

				return new DistinctNode(child, names);
			}
		}

		#endregion

		#region EquiJoinNodeSerializer

		class EquiJoinNodeSerializer : QueryPlanNodeSerializer<EquiJoinNode> {
			public override void Serialize(EquiJoinNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Left);
				WriteChildNode(writer, node.Right);

				WriteObjectNames(node.LeftColumns, writer);
				WriteObjectNames(node.RightColumns, writer);
			}

			public override EquiJoinNode Deserialize(BinaryReader reader) {
				var leftNode = ReadChildNode(reader);
				var rightNode = ReadChildNode(reader);

				var leftColNames = ReadObjectNames(reader);
				var rightColNames = ReadObjectNames(reader);

				return new EquiJoinNode(leftNode, rightNode, leftColNames, rightColNames);
			}
		}

		#endregion

		#region ExhaustiveSelectNodeSerializer

		class ExhaustiveSelectNodeSerializer : QueryPlanNodeSerializer<ExhaustiveSelectNode> {
			public override void Serialize(ExhaustiveSelectNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				SqlExpression.Serialize(node.Expression, writer);
			}

			public override ExhaustiveSelectNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var expression = SqlExpression.Deserialize(reader);

				return new ExhaustiveSelectNode(child, expression);
			}
		}

		#endregion

		#region FetchTableNodeSerializer

		class FetchTableNodeSerializer : QueryPlanNodeSerializer<FetchTableNode> {
			public override void Serialize(FetchTableNode node, BinaryWriter writer) {
				ObjectName.Serialize(node.TableName, writer);
				ObjectName.Serialize(node.AliasName, writer);
			}

			public override FetchTableNode Deserialize(BinaryReader reader) {
				var tableName = ObjectName.Deserialize(reader);
				var alias = ObjectName.Deserialize(reader);

				return new FetchTableNode(tableName, alias);
			}
		}

		#endregion

		#region FetchViewNodeSerializer


		class FetchViewNodeSerializer : QueryPlanNodeSerializer<FetchViewNode> {
			public override void Serialize(FetchViewNode node, BinaryWriter writer) {
				ObjectName.Serialize(node.ViewName, writer);
				ObjectName.Serialize(node.AliasName, writer);
			}

			public override FetchViewNode Deserialize(BinaryReader reader) {
				var viewName = ObjectName.Deserialize(reader);
				var aliasName = ObjectName.Deserialize(reader);

				return new FetchViewNode(viewName, aliasName);
			}
		}

		#endregion

		#region GroupNodeSerializer

		class GroupNodeSerializer : QueryPlanNodeSerializer<GroupNode> {
			public override void Serialize(GroupNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				WriteObjectNames(node.ColumnNames, writer);
				
				ObjectName.Serialize(node.GroupMaxColumn, writer);

				WriteExpressions(node.Functions, writer);
				WriteStrings(node.Names, writer);
			}

			public override GroupNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var colNames = ReadObjectNames(reader);
				var groupMax = ObjectName.Deserialize(reader);
				var functions = ReadExpressions(reader);
				var names = ReadStrings(reader);

				return new GroupNode(child, colNames, groupMax, functions, names);
			}
		}

		#endregion

		#region JoinNodeSerializer

		class JoinNodeSerializer : QueryPlanNodeSerializer<JoinNode> {
			public override void Serialize(JoinNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Left);
				WriteChildNode(writer, node.Right);

				ObjectName.Serialize(node.LeftColumnName, writer);

				writer.Write((byte)node.Operator);

				SqlExpression.Serialize(node.RightExpression, writer);
			}

			public override JoinNode Deserialize(BinaryReader reader) {
				var left = ReadChildNode(reader);
				var right = ReadChildNode(reader);

				var leftColumnName = ObjectName.Deserialize(reader);
				var op = (SqlExpressionType) reader.ReadByte();
				var rightExpression = SqlExpression.Deserialize(reader);

				return new JoinNode(left, right, leftColumnName, op, rightExpression);
			}
		}

		#endregion 

		#region LeftOuterJoinNodeSerializer

		class LeftOuterJoinNodeSerializer : QueryPlanNodeSerializer<LeftOuterJoinNode> {
			public override void Serialize(LeftOuterJoinNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				writer.Write(node.MarkerName);
			}

			public override LeftOuterJoinNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var markerName = reader.ReadString();

				return new LeftOuterJoinNode(child, markerName);
			}
		}

		#endregion

		#region LogicalUnionNodeSerializer

		class LogicalUnionNodeSerializer : QueryPlanNodeSerializer<LogicalUnionNode> {
			public override void Serialize(LogicalUnionNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Left);
				WriteChildNode(writer, node.Right);
			}

			public override LogicalUnionNode Deserialize(BinaryReader reader) {
				var left = ReadChildNode(reader);
				var right = ReadChildNode(reader);

				return new LogicalUnionNode(left, right);
			}
		}

		#endregion

		#region MarkerNodeSerializer

		class MarkerNodeSerializer : QueryPlanNodeSerializer<MarkerNode> {
			public override void Serialize(MarkerNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				writer.Write(node.MarkName);
			}

			public override MarkerNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var markerName = reader.ReadString();

				return new MarkerNode(child, markerName);
			}
		}

		#endregion

		#region NaturalJoinNodeSerializer

		class NaturalJoinNodeSerializer : QueryPlanNodeSerializer<NaturalJoinNode> {
			public override void Serialize(NaturalJoinNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Left);
				WriteChildNode(writer, node.Right);
			}

			public override NaturalJoinNode Deserialize(BinaryReader reader) {
				var left = ReadChildNode(reader);
				var right = ReadChildNode(reader);

				return new NaturalJoinNode(left, right);
			}
		}
		
		#endregion

		#region NonCorrelatedAnyAllNodeSerializer

		class NonCorrelatedAnyAllNodeSerializer : QueryPlanNodeSerializer<NonCorrelatedAnyAllNode> {
			public override void Serialize(NonCorrelatedAnyAllNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Left);
				WriteChildNode(writer, node.Right);

				WriteObjectNames(node.LeftColumnNames, writer);
				writer.Write((byte)node.SubQueryType);
			}

			public override NonCorrelatedAnyAllNode Deserialize(BinaryReader reader) {
				var left = ReadChildNode(reader);
				var right = ReadChildNode(reader);

				var columnNames = ReadObjectNames(reader);
				var subQueryType = (SqlExpressionType) reader.ReadByte();

				return new NonCorrelatedAnyAllNode(left, right, columnNames, subQueryType);
			}
		}

		#endregion

		#region RangeSelectNodeSerializer

		class RageSelectNodeSerializer : QueryPlanNodeSerializer<RangeSelectNode> {
			public override void Serialize(RangeSelectNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				SqlExpression.Serialize(node.Expression, writer);
			}

			public override RangeSelectNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var expression = SqlExpression.Deserialize(reader);

				return new RangeSelectNode(child, expression);
			}
		}

		#endregion

		#region SimplePatternSelectNodeSerializer

		class SimplePatternSelectNodeSerializer : QueryPlanNodeSerializer<SimplePatternSelectNode> {
			public override void Serialize(SimplePatternSelectNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				SqlExpression.Serialize(node.Expression, writer);
			}

			public override SimplePatternSelectNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var expression = SqlExpression.Deserialize(reader);

				return new SimplePatternSelectNode(child, expression);
			}
		}

		#endregion

		#region SimpleSelectNodeSerializer

		class SimpleSelectNodeSerializer : QueryPlanNodeSerializer<SimpleSelectNode> {
			public override void Serialize(SimpleSelectNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				ObjectName.Serialize(node.ColumnName, writer);
				writer.Write((byte)node.OperatorType);
				SqlExpression.Serialize(node.Expression, writer);
			}

			public override SimpleSelectNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var columnName = ObjectName.Deserialize(reader);
				var opType = (SqlExpressionType) reader.ReadByte();
				var expression = SqlExpression.Deserialize(reader);

				return new SimpleSelectNode(child, columnName, opType, expression);
			}
		}

		#endregion

		#region SingleRowTableNodeSerializer

		class SingleRowTableNodeSerializer : QueryPlanNodeSerializer<SingleRowTableNode> {
			public override void Serialize(SingleRowTableNode node, BinaryWriter writer) {
			}

			public override SingleRowTableNode Deserialize(BinaryReader reader) {
				return new SingleRowTableNode();
			}
		}

		#endregion

		#region SortNodeSerializer

		class SortNodeSerializer : QueryPlanNodeSerializer<SortNode> {
			public override void Serialize(SortNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				WriteObjectNames(node.ColumnNames, writer);
				WriteArray(node.Ascending, writer, (b, binaryWriter) => binaryWriter.Write(b));
			}

			public override SortNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var columnNames = ReadObjectNames(reader);
				var ascending = ReadArray(reader, binaryReader => binaryReader.ReadBoolean());

				return new SortNode(child, columnNames, ascending);
			}
		}

		#endregion

		#region SubsetNodeSerializer

		class SubsetNodeSerializer : QueryPlanNodeSerializer<SubsetNode> {
			public override void Serialize(SubsetNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				WriteObjectNames(node.OriginalColumnNames, writer);
				WriteObjectNames(node.AliasColumnNames, writer);
			}

			public override SubsetNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var columnNames = ReadObjectNames(reader);
				var aliasNames = ReadObjectNames(reader);

				return new SubsetNode(child, columnNames, aliasNames);
			}
		}

		#endregion
	}
}
