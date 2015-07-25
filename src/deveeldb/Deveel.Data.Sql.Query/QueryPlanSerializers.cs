using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

using Deveel.Data.DbSystem;
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
			var seriializer = Resolver.ResolveSerializer(nodeType) as IQueryPlanNodeBinarySerializer;
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
			var seriializer = Resolver.ResolveSerializer(nodeType) as IQueryPlanNodeBinarySerializer;
			if (seriializer == null)
				throw new InvalidOperationException(String.Format("Could not find any serializer for type '{0}'.", nodeType));

			return seriializer.Deserialize(reader);
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
			var type = Type.GetType(typeString, true, true);

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

		#region IQueryPlanNodeBinarySerializer

		interface IQueryPlanNodeBinarySerializer : IObjectSerializer {
			void Serialize(IQueryPlanNode queryPlan, BinaryWriter writer);

			IQueryPlanNode Deserialize(BinaryReader reader);
		}

		#endregion

		#region QueryPlanNodeSerializerResolver

		class QueryPlanNodeSerializerResolver : IObjectSerializerResolver {
			private readonly Container container;

			public QueryPlanNodeSerializerResolver() {
				container = new Container();
				Init();
			}

			private void Register<TSerializer, TNode>() where TSerializer : IObjectSerializer where TNode : IQueryPlanNode {
				container.Register<IObjectSerializer, TSerializer>(serviceKey:typeof(TNode).FullName);
			}

			private void Init() {
				Register<CacheNodePointSerializer, CachePointNode>();
				Register<CompositeNodeSerializer, CompositeNode>();
				Register<ConstantSelectNodeSerializer, ConstantSelectNode>();
				Register<CreateFunctionNodeSerializer, CreateFunctionsNode>();
				Register<DistinctNodeSerializer, DistinctNode>();
				Register<EquiJoinNodeSerializer, EquiJoinNode>();
				Register<ExhaustiveSelectNodeSerializer, ExhaustiveSelectNode>();
				Register<FetchTableNodeSerializer, FetchTableNode>();
				Register<FetchViewNodeSerializer, FetchViewNode>();
				Register<GroupNodeSerializer, GroupNode>();
				Register<JoinNodeSerializer, JoinNode>();
				Register<LeftOuterJoinNodeSerializer, LeftOuterJoinNode>();
				Register<LogicalUnionNodeSerializer, LogicalUnionNode>();
				Register<MarkerNodeSerializer, MarkerNode>();
				Register<NaturalJoinNodeSerializer, NaturalJoinNode>();
				Register<NonCorrelatedAnyAllNodeSerializer, NonCorrelatedAnyAllNode>();
				Register<RageSelectNodeSerializer, RangeSelectNode>();
				Register<SimplePatternSelectNodeSerializer, SimplePatternSelectNode>();
				Register<SimpleSelectNodeSerializer, SimpleSelectNode>();
				Register<SingleRowTableNodeSerializer, SingleRowTableNode>();
				Register<SortNodeSerializer, SortNode>();
				Register<SubsetNodeSerializer, SubsetNode>();
			}

			public IObjectSerializer ResolveSerializer(Type objectType) {
				var fullName = objectType.FullName;
				return container.Resolve<IObjectSerializer>(fullName);
			}
		}

		#endregion

		#region QueryPlanNodeSerializer

		abstract class QueryPlanNodeSerializer<TNode> : IQueryPlanNodeBinarySerializer where TNode : class, IQueryPlanNode {
			void IObjectSerializer.Serialize(object obj, Stream outputStream) {
				Serialize((TNode)obj, new BinaryWriter(outputStream, Encoding.Unicode));
			}

			object IObjectSerializer.Deserialize(Stream inputStream) {
				return Deserialize(new BinaryReader(inputStream, Encoding.Unicode));
			}

			protected abstract void Serialize(TNode node, BinaryWriter writer);

			void IQueryPlanNodeBinarySerializer.Serialize(IQueryPlanNode queryPlan, BinaryWriter writer) {
				Serialize((TNode)queryPlan, writer);
			}

			IQueryPlanNode IQueryPlanNodeBinarySerializer.Deserialize(BinaryReader reader) {
				return Deserialize(reader);
			}

			protected abstract TNode Deserialize(BinaryReader reader);
		}

		#endregion

		#region CacheNodePointSerializer

		class CacheNodePointSerializer : QueryPlanNodeSerializer<CachePointNode> {
			protected override void Serialize(CachePointNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				writer.Write(node.Id);
			}

			protected override CachePointNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var id = reader.ReadInt64();
				return new CachePointNode(child, id);
			}
		}

		#endregion

		#region CompositeNodeSerializer

		class CompositeNodeSerializer : QueryPlanNodeSerializer<CompositeNode> {
			protected override void Serialize(CompositeNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Left);
				WriteChildNode(writer, node.Right);
				
				writer.Write(node.All);
				writer.Write((byte)node.CompositeFunction);
			}

			protected override CompositeNode Deserialize(BinaryReader reader) {
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
			protected override void Serialize(ConstantSelectNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				SqlExpression.Serialize(node.Expression, writer);
			}

			protected override ConstantSelectNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var expression = SqlExpression.Deserialize(reader);

				return new ConstantSelectNode(child, expression);
			}
		}

		#endregion

		#region CreateFunctionNodeSerializer

		class CreateFunctionNodeSerializer : QueryPlanNodeSerializer<CreateFunctionsNode> {
			protected override void Serialize(CreateFunctionsNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				WriteExpressions(node.Functions, writer);
				WriteStrings(node.Names, writer);				
			}

			protected override CreateFunctionsNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);

				var functions = ReadExpressions(reader);
				var names = ReadStrings(reader);

				return new CreateFunctionsNode(child, functions, names);
			}
		}

		#endregion

		#region DistinctNodeSerializer

		class DistinctNodeSerializer : QueryPlanNodeSerializer<DistinctNode> {
			protected override void Serialize(DistinctNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				WriteObjectNames(node.ColumnNames, writer);
			}

			protected override DistinctNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var names = ReadObjectNames(reader);

				return new DistinctNode(child, names);
			}
		}

		#endregion

		#region EquiJoinNodeSerializer

		class EquiJoinNodeSerializer : QueryPlanNodeSerializer<EquiJoinNode> {
			protected override void Serialize(EquiJoinNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Left);
				WriteChildNode(writer, node.Right);

				WriteObjectNames(node.LeftColumns, writer);
				WriteObjectNames(node.RightColumns, writer);
			}

			protected override EquiJoinNode Deserialize(BinaryReader reader) {
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
			protected override void Serialize(ExhaustiveSelectNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				SqlExpression.Serialize(node.Expression, writer);
			}

			protected override ExhaustiveSelectNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var expression = SqlExpression.Deserialize(reader);

				return new ExhaustiveSelectNode(child, expression);
			}
		}

		#endregion

		#region FetchTableNodeSerializer

		class FetchTableNodeSerializer : QueryPlanNodeSerializer<FetchTableNode> {
			protected override void Serialize(FetchTableNode node, BinaryWriter writer) {
				ObjectName.Serialize(node.TableName, writer);
				ObjectName.Serialize(node.AliasName, writer);
			}

			protected override FetchTableNode Deserialize(BinaryReader reader) {
				var tableName = ObjectName.Deserialize(reader);
				var alias = ObjectName.Deserialize(reader);

				return new FetchTableNode(tableName, alias);
			}
		}

		#endregion

		#region FetchViewNodeSerializer


		class FetchViewNodeSerializer : QueryPlanNodeSerializer<FetchViewNode> {
			protected override void Serialize(FetchViewNode node, BinaryWriter writer) {
				ObjectName.Serialize(node.ViewName, writer);
				ObjectName.Serialize(node.AliasName, writer);
			}

			protected override FetchViewNode Deserialize(BinaryReader reader) {
				var viewName = ObjectName.Deserialize(reader);
				var aliasName = ObjectName.Deserialize(reader);

				return new FetchViewNode(viewName, aliasName);
			}
		}

		#endregion

		#region GroupNodeSerializer

		class GroupNodeSerializer : QueryPlanNodeSerializer<GroupNode> {
			protected override void Serialize(GroupNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				WriteObjectNames(node.ColumnNames, writer);
				
				ObjectName.Serialize(node.GroupMaxColumn, writer);

				WriteExpressions(node.Functions, writer);
				WriteStrings(node.Names, writer);
			}

			protected override GroupNode Deserialize(BinaryReader reader) {
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
			protected override void Serialize(JoinNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Left);
				WriteChildNode(writer, node.Right);

				ObjectName.Serialize(node.LeftColumnName, writer);

				writer.Write((byte)node.Operator);

				SqlExpression.Serialize(node.RightExpression, writer);
			}

			protected override JoinNode Deserialize(BinaryReader reader) {
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
			protected override void Serialize(LeftOuterJoinNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				writer.Write(node.MarkerName);
			}

			protected override LeftOuterJoinNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var markerName = reader.ReadString();

				return new LeftOuterJoinNode(child, markerName);
			}
		}

		#endregion

		#region LogicalUnionNodeSerializer

		class LogicalUnionNodeSerializer : QueryPlanNodeSerializer<LogicalUnionNode> {
			protected override void Serialize(LogicalUnionNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Left);
				WriteChildNode(writer, node.Right);
			}

			protected override LogicalUnionNode Deserialize(BinaryReader reader) {
				var left = ReadChildNode(reader);
				var right = ReadChildNode(reader);

				return new LogicalUnionNode(left, right);
			}
		}

		#endregion

		#region MarkerNodeSerializer

		class MarkerNodeSerializer : QueryPlanNodeSerializer<MarkerNode> {
			protected override void Serialize(MarkerNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				writer.Write(node.MarkName);
			}

			protected override MarkerNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var markerName = reader.ReadString();

				return new MarkerNode(child, markerName);
			}
		}

		#endregion

		#region NaturalJoinNodeSerializer

		class NaturalJoinNodeSerializer : QueryPlanNodeSerializer<NaturalJoinNode> {
			protected override void Serialize(NaturalJoinNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Left);
				WriteChildNode(writer, node.Right);
			}

			protected override NaturalJoinNode Deserialize(BinaryReader reader) {
				var left = ReadChildNode(reader);
				var right = ReadChildNode(reader);

				return new NaturalJoinNode(left, right);
			}
		}
		
		#endregion

		#region NonCorrelatedAnyAllNodeSerializer

		class NonCorrelatedAnyAllNodeSerializer : QueryPlanNodeSerializer<NonCorrelatedAnyAllNode> {
			protected override void Serialize(NonCorrelatedAnyAllNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Left);
				WriteChildNode(writer, node.Right);

				WriteObjectNames(node.LeftColumnNames, writer);
				writer.Write((byte)node.SubQueryType);
			}

			protected override NonCorrelatedAnyAllNode Deserialize(BinaryReader reader) {
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
			protected override void Serialize(RangeSelectNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				SqlExpression.Serialize(node.Expression, writer);
			}

			protected override RangeSelectNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var expression = SqlExpression.Deserialize(reader);

				return new RangeSelectNode(child, expression);
			}
		}

		#endregion

		#region SimplePatternSelectNodeSerializer

		class SimplePatternSelectNodeSerializer : QueryPlanNodeSerializer<SimplePatternSelectNode> {
			protected override void Serialize(SimplePatternSelectNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				SqlExpression.Serialize(node.Expression, writer);
			}

			protected override SimplePatternSelectNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var expression = SqlExpression.Deserialize(reader);

				return new SimplePatternSelectNode(child, expression);
			}
		}

		#endregion

		#region SimpleSelectNodeSerializer

		class SimpleSelectNodeSerializer : QueryPlanNodeSerializer<SimpleSelectNode> {
			protected override void Serialize(SimpleSelectNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				ObjectName.Serialize(node.ColumnName, writer);
				writer.Write((byte)node.OperatorType);
				SqlExpression.Serialize(node.Expression, writer);
			}

			protected override SimpleSelectNode Deserialize(BinaryReader reader) {
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
			protected override void Serialize(SingleRowTableNode node, BinaryWriter writer) {
			}

			protected override SingleRowTableNode Deserialize(BinaryReader reader) {
				return new SingleRowTableNode();
			}
		}

		#endregion

		#region SortNodeSerializer

		class SortNodeSerializer : QueryPlanNodeSerializer<SortNode> {
			protected override void Serialize(SortNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				WriteObjectNames(node.ColumnNames, writer);
				WriteArray(node.Ascending, writer, (b, binaryWriter) => binaryWriter.Write(b));
			}

			protected override SortNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var columnNames = ReadObjectNames(reader);
				var ascending = ReadArray(reader, binaryReader => binaryReader.ReadBoolean());

				return new SortNode(child, columnNames, ascending);
			}
		}

		#endregion

		#region SubsetNodeSerializer

		class SubsetNodeSerializer : QueryPlanNodeSerializer<SubsetNode> {
			protected override void Serialize(SubsetNode node, BinaryWriter writer) {
				WriteChildNode(writer, node.Child);
				WriteObjectNames(node.OriginalColumnNames, writer);
				WriteObjectNames(node.AliasColumnNames, writer);
			}

			protected override SubsetNode Deserialize(BinaryReader reader) {
				var child = ReadChildNode(reader);
				var columnNames = ReadObjectNames(reader);
				var aliasNames = ReadObjectNames(reader);

				return new SubsetNode(child, columnNames, aliasNames);
			}
		}

		#endregion
	}
}
