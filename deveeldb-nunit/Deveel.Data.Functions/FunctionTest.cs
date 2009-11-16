using System;
using System.Collections;

using NUnit.Framework;

namespace Deveel.Data.Functions {
	[TestFixture]
	public sealed class FunctionTest {
		#region Aritmetic Functions

		[Test]
		public void Abs() {
			Expression exp = Expression.Parse("ABS(-45)");
			TObject result = exp.Evaluate(null, null, null);
			Assert.IsTrue(result == 45);

			exp = Expression.Parse("ABS(-5673.9049)");
			result = exp.Evaluate(null, null, null);
			Assert.IsTrue(result == TObject.GetDouble(5673.9049));
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
			groupResolver.AddToGroup(0, "a", 34);
			groupResolver.AddToGroup(1, "a", 56);
			groupResolver.AddToGroup(2, "a", 11);
			groupResolver.AddToGroup(3, "a", 89);
			groupResolver.AddToGroup(0, "b", 44);
			groupResolver.AddToGroup(1, "b", 154);
			groupResolver.AddToGroup(2, "b", 27);
			groupResolver.AddToGroup(3, "b", 564);
			Expression exp = Expression.Parse("AVG(a)");
			TObject result = exp.Evaluate(groupResolver, null, null);
			Assert.IsTrue(result == 47.5);

			exp = Expression.Parse("AVG(b)");
			result = exp.Evaluate(groupResolver, null, null);
			Assert.IsTrue(result == 197.25);
		}

		[Test]
		public void Count() {
			GroupResolver groupResolver = new GroupResolver();
			groupResolver.AddToGroup(0, "a", 34);
			groupResolver.AddToGroup(1, "a", 56);
			groupResolver.AddToGroup(2, "a", 11);
			groupResolver.AddToGroup(3, "a", 89);
			Expression exp = Expression.Parse("COUNT(a)");
			TObject result = exp.Evaluate(groupResolver, null, null);
			Assert.IsTrue(result == 4);

			exp = Expression.Parse("COUNT(*)");
			result = exp.Evaluate(groupResolver, null, null);
			Assert.IsTrue(result == 4);
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

			private void AddToGroup(int setIndex, Variable variable, TObject value) {
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
				AddToGroup(setIndex, Variable.Resolve(variable), value);
			}

			public TObject Resolve(Variable variable, int set_index) {
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

				public TObject Resolve(Variable variable) {
					return values[variable] as TObject;
				}

				public TType ReturnTType(Variable variable) {
					TObject obj = values[variable] as TObject;
					return obj == null ? TType.NullType : obj.TType;
				}
			}
		}

		#endregion
	}
}