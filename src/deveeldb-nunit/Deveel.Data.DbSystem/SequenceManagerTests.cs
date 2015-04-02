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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Index;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Transactions;

using Moq;

using NUnit.Framework;

namespace Deveel.Data.DbSystem {
	[TestFixture]
	public sealed class SequenceManagerTests {
		private IDictionary<ObjectName, IDbObject> objects; 

		private void MockSequenceTable() {
			var rows = new List<Row>();
			var mock = new Mock<IMutableTable>();
			mock.Setup(x => x.FullName)
				.Returns(SystemSchema.SequenceTableName);
			mock.Setup(x => x.ObjectType)
				.Returns(DbObjectType.Table);
			mock.Setup(x => x.TableInfo)
				.Returns(SystemSchema.SequenceTableInfo);
			mock.Setup(x => x.RowCount)
				.Returns(() => rows.Count);
			mock.Setup(x => x.NewRow())
				.Returns(() => {
					var row = new Row(mock.Object, new RowId(1, rows.Count));
					rows.Add(row);
					return row;
				});
			mock.Setup(x => x.AddRow(It.IsAny<Row>()))
				.Callback<Row>(row => {
					rows.Add(row);
				});
			mock.Setup(x => x.UpdateRow(It.IsAny<Row>()))
				.Callback<Row>(row => {
					var index = rows.FindIndex(y => y.RowId.Equals(row.RowId));
					if (index == -1)
						throw new ArgumentException();

					rows[index] = row;
				});
			mock.Setup(x => x.GetValue(It.IsAny<long>(), It.IsAny<int>()))
				.Returns<long, int>((rowNum, colIndex) => {
					var row = rows.FirstOrDefault(x => x.RowId.RowNumber == rowNum);
					if (row == null)
						return DataObject.Null();

					return row.GetValue(colIndex);
				});
			mock.Setup(x => x.GetEnumerator())
				.Returns(() => new SimpleRowEnumerator(mock.Object));
			mock.Setup(x => x.GetIndex(It.IsAny<int>()))
				.Returns<int>(columnOffset => new BlindSearchIndex(mock.Object, columnOffset));

			objects[SystemSchema.SequenceTableName] = mock.Object;
		}

		private void MockSequenceInfoTable() {
			var rows = new List<Row>();
			var mock = new Mock<IMutableTable>();
			mock.Setup(x => x.FullName)
				.Returns(SystemSchema.SequenceInfoTableName);
			mock.Setup(x => x.ObjectType)
				.Returns(DbObjectType.Table);
			mock.Setup(x => x.TableInfo)
				.Returns(SystemSchema.SequenceInfoTableInfo);
			mock.Setup(x => x.RowCount)
				.Returns(() => rows.Count);
			mock.Setup(x => x.NewRow())
				.Returns(() => {
					var row = new Row(mock.Object, new RowId(2, rows.Count));
					rows.Add(row);
					return row;
				});
			mock.Setup(x => x.AddRow(It.IsAny<Row>()))
				.Callback<Row>(row => {
					rows.Add(row);
				});
			mock.Setup(x => x.UpdateRow(It.IsAny<Row>()))
				.Callback<Row>(row => {
					var index = rows.FindIndex(y => y.RowId.Equals(row.RowId));
					if (index == -1)
						throw new ArgumentException();

					rows[index] = row;
				});
			mock.Setup(x => x.GetEnumerator())
				.Returns(() => new SimpleRowEnumerator(mock.Object));
			mock.Setup(x => x.GetValue(It.IsAny<long>(), It.IsAny<int>()))
				.Returns<long, int>((rowNum, colIndex) => {
					var row = rows.FirstOrDefault(x => x.RowId.RowNumber == rowNum);
					if (row == null)
						return DataObject.Null();

					return row.GetValue(colIndex);
				});
			mock.Setup(x => x.GetIndex(It.IsAny<int>()))
				.Returns<int>(columnOffset => new BlindSearchIndex(mock.Object, columnOffset));

			objects[SystemSchema.SequenceInfoTableName] = mock.Object;
		}

		[SetUp]
		public void TestSetup() {
			objects = new Dictionary<ObjectName, IDbObject>();

			var ids = new Dictionary<ObjectName, int>();

			MockSequenceTable();
			MockSequenceInfoTable();

			var factoryMock = new Mock<ITransactionContext>();

			var resolverMock = new Mock<IObjectManagerResolver>();
			resolverMock.Setup(x => x.GetManagers())
				.Returns(() => {
					return new List<IObjectManager> {
						new SequenceManager(transaction),
						new TableManager(transaction)
					};
				});
			resolverMock.Setup(x => x.ResolveForType(It.IsAny<DbObjectType>()))
				.Returns<DbObjectType>(x => {
					if (x == DbObjectType.Sequence)
						return new SequenceManager(transaction);
					if (x == DbObjectType.Table)
						return new TableManager(transaction);
					return null;
				});

			var tnxMock = new Mock<ITransaction>();
			tnxMock.Setup(x => x.NextTableId(It.IsAny<ObjectName>()))
				.Returns<ObjectName>(name => {
					int value;
					int id = -1;
					if (ids.TryGetValue(name, out value)) {
						id = value;
					}

					id++;
					ids[name] = id;
					return new SqlNumber(id);
				});
			tnxMock.Setup(x => x.Context)
				.Returns(factoryMock.Object);
			tnxMock.Setup(x => x.ObjectManagerResolver)
				.Returns(() => resolverMock.Object);

			factoryMock.Setup(x => x.CreateTransaction(It.IsAny<TransactionIsolation>()))
				.Returns<TransactionIsolation>(isolation => tnxMock.Object);

			transaction = tnxMock.Object;
		}

		private ITransaction transaction;

		[Test]
		public void CreateNormalSequence() {
			var sequenceManager = new SequenceManager(transaction);

			var sequenceName = ObjectName.Parse("APP.test_sequence");
			var seqInfo = new SequenceInfo(sequenceName, new SqlNumber(0), new SqlNumber(1), new SqlNumber(0), new SqlNumber(Int64.MaxValue), 126);

			ISequence sequence =null;
			Assert.DoesNotThrow(() => sequence = sequenceManager.CreateSequence(seqInfo));
			Assert.IsNotNull(sequence);
		}

		[Test]
		public void CreateNativeSequence() {
			
		}

		[Test]
		public void IncremementSequenceValue() {
			var sequenceManager = new SequenceManager(transaction);

			var sequenceName = ObjectName.Parse("APP.test_sequence");
			var seqInfo = new SequenceInfo(sequenceName, new SqlNumber(0), new SqlNumber(1), new SqlNumber(0), new SqlNumber(Int64.MaxValue), 126);

			ISequence sequence = null;
			Assert.DoesNotThrow(() => sequence = sequenceManager.CreateSequence(seqInfo));
			Assert.IsNotNull(sequence);

			SqlNumber currentValue = SqlNumber.Null;
			Assert.DoesNotThrow(() => currentValue = sequence.NextValue());
			Assert.IsNotNull(currentValue);
			Assert.AreEqual(new SqlNumber(0), currentValue);
		}
	}
}
