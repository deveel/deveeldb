using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using Deveel.Data.Query;
using Deveel.Data.Services;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Methods;
using Deveel.Data.Sql.Tables.Model;
using Deveel.Data.Sql.Types;

using Moq;

using Xunit;

namespace Deveel.Data.Sql.Tables {
	public class FunctionTableTests {
		private IContext context;
		private TemporaryTable left;

		public FunctionTableTests() {
			var leftInfo = new TableInfo(ObjectName.Parse("tab1"));
			leftInfo.Columns.Add(new ColumnInfo("a", PrimitiveTypes.Integer()));
			leftInfo.Columns.Add(new ColumnInfo("b", PrimitiveTypes.Boolean()));

			left = new TemporaryTable(leftInfo);

			left.AddRow(new[] {SqlObject.Integer(23), SqlObject.Boolean(true)});
			left.AddRow(new[] {SqlObject.Integer(54), SqlObject.Boolean(null)});

			left.BuildIndex();

			var scope = new ServiceContainer();

			var mock = new Mock<IContext>();
			mock.SetupGet(x => x.Scope)
				.Returns(scope);
			mock.Setup(x => x.Dispose())
				.Callback(scope.Dispose);

			context = mock.Object;

			var group = new List<SqlObject> {
				SqlObject.Integer(33),
				SqlObject.Integer(22),
				SqlObject.Integer(1)

			};

			var refResolver = new Mock<IReferenceResolver>();
			refResolver.Setup(x => x.ResolveType(It.IsAny<ObjectName>()))
				.Returns<ObjectName>(name => PrimitiveTypes.Integer());
			refResolver.Setup(x => x.ResolveReferenceAsync(It.IsAny<ObjectName>()))
				.Returns<ObjectName>(name => Task.FromResult(group[0]));

			var resolverMock = new Mock<IGroupResolver>();
			resolverMock.SetupGet(x => x.Size)
				.Returns(group.Count);
			resolverMock.Setup(x => x.ResolveReferenceAsync(It.IsAny<ObjectName>(), It.IsAny<long>()))
				.Returns<ObjectName, long>((name, index) => Task.FromResult(group[(int) index]));
			resolverMock.Setup(x => x.GetResolver(It.IsAny<long>()))
				.Returns(refResolver.Object);

			// scope.AddMethodRegistry<SystemFunctionProvider>();
			scope.RegisterInstance<IGroupResolver>(resolverMock.Object);
		}

		[Fact]
		public async Task CreateNewFunctionTable() {
			var exp = SqlExpression.Equal(SqlExpression.Reference(ObjectName.Parse("tab1.a")),
				SqlExpression.Constant(SqlObject.Integer(2)));

			var cols = new[] {
				new FunctionColumnInfo(exp, "exp1", PrimitiveTypes.Integer())
			};

			var table = new FunctionTable(context, left, cols);

			Assert.NotNull(table.TableInfo);
			Assert.Equal(2, table.RowCount);

			var value = await table.GetValueAsync(0, 0);

			Assert.NotNull(value);
			Assert.True(value.IsFalse);
		}

		[Fact]
		public async Task GroupMax() {
			var exp = SqlExpression.Equal(SqlExpression.Reference(ObjectName.Parse("tab1.a")),
				SqlExpression.Constant(SqlObject.Integer(2)));

			var cols = new[] {
				new FunctionColumnInfo(exp, "exp1", PrimitiveTypes.Integer())
			};

			var table = new FunctionTable(context, left, cols);

			var result = table.GroupMax(new ObjectName("b"));

			Assert.NotNull(result);

			var value1 = await result.GetValueAsync(0, 0);

			Assert.NotNull(value1);
			Assert.Equal(SqlObject.Integer(23), value1);
		}

		// TODO:
		//[Fact]
		//public async Task GroupByCount() {
		//	var exp = SqlExpression.Function(new ObjectName("count"),
		//		new InvokeArgument(SqlExpression.Reference(ObjectName.Parse("tab1.a"))));

		//	var cols = new[] {
		//		new FunctionColumnInfo(exp, "exp1", PrimitiveTypes.Integer())
		//	};

		//	var table = new GroupTable(context, left, cols, new[] {ObjectName.Parse("tab1.a")});
		//	var value1 = await table.GetValueAsync(0, 0);

		//	Assert.NotNull(value1);
		//	Assert.Equal(SqlObject.BigInt(2), value1);
		//}

		[Fact]
		public async Task GroupMaxOverGroupBy() {
			var exp = SqlExpression.Function(new ObjectName("count"),
				new InvokeArgument(SqlExpression.Reference(ObjectName.Parse("tab1.a"))));

			var cols = new[] {
				new FunctionColumnInfo(exp, "exp1", PrimitiveTypes.Integer())
			};

			var table = new GroupTable(context, left, cols, new[] {ObjectName.Parse("tab1.a")});
			var groupMax = table.GroupMax(ObjectName.Parse("tab1.a"));

			Assert.NotNull(groupMax);
			Assert.Equal(1, groupMax.RowCount);

			var value = await groupMax.GetValueAsync(0, 0);

			Assert.NotNull(value);
			Assert.False(value.IsFalse);

			Assert.Equal(SqlObject.Integer(54), value);
		}

		[Fact]
		public void MakeFullGroupTable() {
			var exp = SqlExpression.Function(new ObjectName("count"),
				new InvokeArgument(SqlExpression.Reference(ObjectName.Parse("tab1.a"))));

			var cols = new[] {
				new FunctionColumnInfo(exp, "exp1", PrimitiveTypes.Integer())
			};

			var table = new GroupTable(context, left, cols, new ObjectName[0]);

			Assert.Equal(2, table.RowCount);
		}
	}
}
