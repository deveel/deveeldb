using System;
using System.Collections;

using Deveel.Data.DbSystem;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Functions {
	[TestFixture]
	public sealed class FunctionTest : TestBase {
		#region Aritmetic Functions

		public FunctionTest() 
			: base(StorageType.Memory) {
		}

		[Test]
		public void Abs() {
			Expression exp = Expression.Parse("ABS(-45)");
			TObject result = exp.Evaluate(null, null, null);
			Assert.IsTrue((int)result == 45);

			exp = Expression.Parse("ABS(-5673.9049)");
			result = exp.Evaluate(null, null, null);
			Assert.IsTrue(result == TObject.CreateDouble(5673.9049));
		}

		#endregion

		#region String Functions

		[Test]
		public void Concat() {
			Expression exp = Expression.Parse("CONCAT('str1', '|', 'str2')");
			TObject result = exp.Evaluate(null, null, null);
			string result_str = result.ToStringValue();
			Assert.IsTrue(result_str == "str1|str2");
		}

		#endregion

		#region Aggregate Functions

		[Test]
		public void Avg() {
			GroupResolver groupResolver = new GroupResolver();
			groupResolver.AddToGroup(0, "a", (TObject) 34);
			groupResolver.AddToGroup(1, "a", (TObject) 56);
			groupResolver.AddToGroup(2, "a", (TObject) 11);
			groupResolver.AddToGroup(3, "a", (TObject) 89);
			groupResolver.AddToGroup(0, "b", (TObject) 44);
			groupResolver.AddToGroup(1, "b", (TObject) 154);
			groupResolver.AddToGroup(2, "b", (TObject) 27);
			groupResolver.AddToGroup(3, "b", (TObject) 564);
			Expression exp = Expression.Parse("AVG(a)");
			TObject result = exp.Evaluate(groupResolver, null, null);
			Assert.IsTrue(result == (TObject) 47.5);

			exp = Expression.Parse("AVG(b)");
			result = exp.Evaluate(groupResolver, null, null);
			Assert.IsTrue(result == (TObject) 197.25);
		}

		[Test]
		public void Count() {
			GroupResolver groupResolver = new GroupResolver();
			groupResolver.AddToGroup(0, "a", (TObject) 34);
			groupResolver.AddToGroup(1, "a", (TObject) 56);
			groupResolver.AddToGroup(2, "a", (TObject) 11);
			groupResolver.AddToGroup(3, "a", (TObject) 89);
			Expression exp = Expression.Parse("COUNT(a)");
			TObject result = exp.Evaluate(groupResolver, null, null);
			Assert.IsTrue(result == (TObject) 4);

			exp = Expression.Parse("COUNT(*)");
			result = exp.Evaluate(groupResolver, null, null);
			Assert.IsTrue(result == (TObject) 4);
		}

		#endregion

		#region Time Functions

		[Test]
		public void TimeStamp() {
			Expression exp = Expression.Parse("TIMESTAMP '2000-12-31 23:59:59'");
			TObject result = exp.Evaluate(null, null, null);
			Assert.IsTrue(result.TType is TDateType);
			DateTime dateTime = result.ToDateTime();
			Assert.AreEqual(new DateTime(2000, 12, 31, 23, 59, 59), dateTime);
		}

		[Test]
		public void Extract() {
			Expression exp = Expression.Parse("EXTRACT(YEAR FROM TIMESTAMP '2000-12-31 23:59:59')");
			TObject result = exp.Evaluate(null, null, null);
			Assert.IsTrue(result.TType is TNumericType);
			Assert.AreEqual(2000, result.ToBigNumber().ToInt32());

			exp = Expression.Parse("EXTRACT(MONTH FROM TIMESTAMP '2000-12-31 23:59:59')");
			result = exp.Evaluate(null, null, null);
			Assert.IsTrue(result.TType is TNumericType);
			Assert.AreEqual(12, result.ToBigNumber().ToInt32());
		}

		[Test]
		public void Interval() {
			Expression exp = Expression.Parse("INTERVAL '3' DAY");
			TObject result = exp.Evaluate(null, null, null);
			Assert.IsTrue(result.TType is TIntervalType);
			TimeSpan timeSpan = result.ToTimeSpan();
			Assert.AreEqual(3, timeSpan.TotalDays);
		}

		#endregion

		#region MiscFunctions

		[Test]
		public void NullIf() {
			TObject result = Expression.Evaluate("NULLIF('test1', 'test2')");
			Assert.IsTrue(TObject.CreateString("test1") == result);

			result = Expression.Evaluate("NULLIF(24 * 2, 48)");
			Assert.IsTrue(TObject.Null == result);

			result = Expression.Evaluate("NULLIF(3, 5)");
			Assert.AreEqual(3, result);
		}

		[Test]
		public void ExistsFunction() {
			const string sql = "EXISTS(SELECT * FROM Person)";

			DatabaseConnection connection = CreateDatabaseConnection();
			DatabaseQueryContext queryContext = new DatabaseQueryContext(connection);
			Expression exp = Expression.Parse(sql);
			TObject result = exp.Evaluate(null, queryContext);
			
			Console.Out.WriteLine("{0} = {1}", sql, result);
		}

		#endregion

		#region GroupResolver

		private class GroupResolver : IGroupResolver {
			public GroupResolver() {
				sets = new ArrayList();
			}

			private readonly ArrayList sets;

			public int GroupId {
				get { return 0; }
			}

			public int Count {
				get { return sets.Count; }
			}

			private void AddToGroup(int setIndex, VariableName variable, TObject value) {
				if (setIndex >= sets.Count) {
					for (int i = sets.Count - 1; i < setIndex; i++)
						sets.Add(new Hashtable());
				}

				Hashtable keyValues = sets[setIndex] as Hashtable;
				if (keyValues == null) {
					keyValues = new Hashtable();
					sets[setIndex] = keyValues;
				}

				keyValues[variable] = value;
			}

			public void AddToGroup(int setIndex, string variable, TObject value) {
				AddToGroup(setIndex, VariableName.Resolve(variable), value);
			}

			public TObject Resolve(VariableName variable, int set_index) {
				IVariableResolver resolver = GetVariableResolver(set_index);
				return resolver.Resolve(variable);
			}

			public IVariableResolver GetVariableResolver(int set_index) {
				Hashtable keyValues = sets[set_index] as Hashtable;
				if (keyValues == null)
					keyValues = new Hashtable();
				return new VariableResolver(set_index, keyValues);
			}

			private class VariableResolver : IVariableResolver {
				public VariableResolver(int set_index, Hashtable values) {
					this.set_index = set_index;
					this.values = values;
				}

				private readonly Hashtable values;
				private readonly int set_index;

				public int SetId {
					get { return set_index; }
				}

				public TObject Resolve(VariableName variable) {
					return values[variable] as TObject;
				}

				public TType ReturnTType(VariableName variable) {
					TObject obj = values[variable] as TObject;
					return obj == null ? TType.NullType : obj.TType;
				}
			}
		}

		#endregion
	}
}